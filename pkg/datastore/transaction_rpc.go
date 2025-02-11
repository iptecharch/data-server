package datastore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sdcio/cache/proto/cachepb"
	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/datastore/types"
	"github.com/sdcio/data-server/pkg/tree"
	"github.com/sdcio/data-server/pkg/utils"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var (
	ErrDatastoreLocked = errors.New("Datastore is locked, other action is ongoing.")
)

// expandAndConvertIntent takes a slice of Updates ([]*sdcpb.Update) and converts it into a tree.UpdateSlice, that contains *cache.Updates.
func (d *Datastore) expandAndConvertIntent(ctx context.Context, intentName string, priority int32, upds []*sdcpb.Update) (tree.UpdateSlice, error) {
	converter := utils.NewConverter(d.getValidationClient())

	// list of updates to be added to the cache
	// Expands the value, in case of json to single typed value updates
	expandedReqUpdates, err := converter.ExpandUpdates(ctx, upds, true)
	if err != nil {
		return nil, err
	}

	// temp storage for cache.Update of the req. They are to be added later.
	newCacheUpdates := make([]*cache.Update, 0, len(expandedReqUpdates))

	for _, u := range expandedReqUpdates {
		pathslice, err := utils.CompletePath(nil, u.GetPath())
		if err != nil {
			return nil, err
		}

		// since we already have the pathslice, we construct the cache.Update, but keep it for later
		// addition to the tree. First we need to mark the existing once for deletion

		// make sure typedValue is carrying the correct type
		err = d.validateUpdate(ctx, u)
		if err != nil {
			return nil, err
		}

		// convert value to []byte for cache insertion
		val, err := proto.Marshal(u.GetValue())
		if err != nil {
			return nil, err
		}

		// construct the cache.Update
		newCacheUpdates = append(newCacheUpdates, cache.NewUpdate(pathslice, val, priority, intentName, 0))
	}
	return newCacheUpdates, nil
}

// SdcpbTransactionIntentToInternalTI converts sdcpb.TransactionIntent to types.TransactionIntent
func (d *Datastore) SdcpbTransactionIntentToInternalTI(ctx context.Context, req *sdcpb.TransactionIntent) (*types.TransactionIntent, error) {

	// create a new TransactionIntent with the given name and priority
	ti := types.NewTransactionIntent(req.GetIntent(), req.GetPriority())

	// setting the provided flags in the TransactionIntent
	if req.GetDelete() {
		ti.SetDeleteFlag()
	}
	if req.GetOrphan() {
		ti.SetDeleteOnlyIntendedFlag()
	}

	// convert the sdcpb.updates to tree.UpdateSlice
	cacheUpdates, err := d.expandAndConvertIntent(ctx, req.GetIntent(), req.GetPriority(), req.GetUpdate())
	if err != nil {
		return nil, err
	}

	// add the intent to the TransactionIntent
	ti.AddUpdates(cacheUpdates)

	return ti, nil
}

// replaceIntent takes a Transaction and treats it as a replaceIntent, replacing the whole device configuration with the content of the given intent.
// returns the warnings as a []string and potential errors that happend during validation / from SBI Set()
func (d *Datastore) replaceIntent(ctx context.Context, transaction *types.Transaction) ([]string, error) {

	// create a new TreeContext
	tc := tree.NewTreeContext(d.treeCacheSchemaClient, d.Name())
	// refresh the Cache content of the treeCacheSchemaClient
	tc.GetTreeSchemaCacheClient().RefreshCaches(ctx)

	// create a new TreeRoot to collect validate and hand to SBI.Set()
	root, err := tree.NewTreeRoot(ctx, tc)
	if err != nil {
		return nil, err
	}

	// set TreeContext actual owner to the const of ReplaceIntentName
	tc.SetActualOwner(tree.ReplaceIntentName)

	// store the actual / old running in the transaction
	runningUpds, err := tc.GetTreeSchemaCacheClient().ReadRunningFull(ctx)
	transaction.GetOldRunning().AddUpdates(runningUpds)

	// creat a InsertFlags struct with the New flag set.
	flagNew := tree.NewUpdateInsertFlags()
	flagNew.SetNewFlag()

	// add all the replace transaction updates with the New flag set
	err = root.AddCacheUpdatesRecursive(ctx, transaction.GetReplace().GetUpdates(), flagNew)
	if err != nil {
		return nil, err
	}

	log.Debugf("Transaction Replace: %s - finish tree insertion phase", transaction.GetTransactionId())
	root.FinishInsertionPhase(ctx)

	// perform validation
	validationResult := root.Validate(ctx, true)
	validationResult.ErrorsStr()
	if validationResult.HasErrors() {
		return nil, validationResult.JoinErrors()
	}

	warnings := validationResult.WarningsStr()

	log.Infof("Transaction: %s - validation passed", transaction.GetTransactionId())

	// we use the TargetSourceReplace, that adjustes the tree results in a way
	// that the whole config tree is getting replaced.
	replaceRoot := types.NewTargetSourceReplace(root)

	// apply the resulting config to the device
	dataResp, err := d.applyIntent(ctx, replaceRoot)
	if err != nil {
		return nil, err
	}
	// collect warnings
	warnings = append(warnings, dataResp.GetWarnings()...)

	// query tree for deletes
	deletes, err := root.GetDeletes(true)
	if err != nil {
		return nil, err
	}

	// fast and optimistic writeback to the config store
	err = d.cacheClient.Modify(ctx, d.Name(), &cache.Opts{
		Store: cachepb.Store_CONFIG,
	}, deletes.PathSlices().ToStringSlice(), root.GetHighestPrecedence(false).ToCacheUpdateSlice())
	if err != nil {
		return nil, fmt.Errorf("failed updating the running config store for %s: %w", d.Name(), err)
	}

	log.Infof("ds=%s transaction=%s applied", d.Name(), transaction.GetTransactionId()+" - replace")

	return warnings, nil
}

// lowlevelTransactionSet
func (d *Datastore) lowlevelTransactionSet(ctx context.Context, transaction *types.Transaction, dryRun bool) (*sdcpb.TransactionSetResponse, error) {

	// create a new TreeContext
	tc := tree.NewTreeContext(d.treeCacheSchemaClient, d.Name())
	// refresh the SchemaClientCache
	tc.GetTreeSchemaCacheClient().RefreshCaches(ctx)

	// creat a new TreeRoot
	root, err := tree.NewTreeRoot(ctx, tc)
	if err != nil {
		return nil, err
	}

	// we need to curate a list of all the paths involved, of the old and new intent contents.
	// this is then used to load the IntendedStore highes prio into the tree, to decide if an update
	// is to be applied or if a higher precedence update exists and is therefore not applicable. Also if the value got
	// deleted and a previousely shadowed entry becomes active.
	involvedPaths := tree.NewPathSet()

	// create a flags attribute
	flagNew := tree.NewUpdateInsertFlags()
	// where the New flag is set
	flagNew.SetNewFlag()

	// iterate through all the intents
	for _, intent := range transaction.GetNewIntents() {
		// update the TreeContext to reflect the actual owner (intent name)
		tc.SetActualOwner(intent.GetName())

		log.Debugf("Transaction: %s - adding intent %s to tree", transaction.GetTransactionId(), intent.GetName())

		// load the old intent content into the tree and return it
		oldIntentContent, err := root.LoadIntendedStoreOwnerData(ctx, intent.GetName(), intent.GetOnlyIntended())
		if err != nil {
			return nil, err
		}

		// store the old intent content in the transaction as the old intent.
		err = transaction.AddIntentContent(intent.GetName(), types.TransactionIntentOld, oldIntentContent.GetFirstPriorityValue(), oldIntentContent)
		if err != nil {
			return nil, err
		}

		// add the content to the Tree
		err = root.AddCacheUpdatesRecursive(ctx, intent.GetUpdates(), flagNew)
		if err != nil {
			return nil, err
		}

		// add the old intent contents paths to the involvedPaths slice
		involvedPaths.Join(oldIntentContent.ToPathSet())
		// add the new intent contents paths to the involvedPaths slice
		involvedPaths.Join(intent.GetUpdates().ToPathSet())
	}

	// load the alternatives for the involved paths into the tree
	err = d.loadIntendedStoreHighestPrio(ctx, d.treeCacheSchemaClient, root, involvedPaths, transaction.GetIntentNames())
	if err != nil {
		return nil, err
	}

	// add running to the tree
	err = d.populateTreeWithRunning(ctx, d.treeCacheSchemaClient, root)
	if err != nil {
		return nil, err
	}

	log.Debugf("Transaction: %s - finish tree insertion phase", transaction.GetTransactionId())
	// FinishInsertion Phase
	root.FinishInsertionPhase(ctx)

	// perform validation
	validationResult := root.Validate(ctx, false)

	// prepare the response struct
	result := &sdcpb.TransactionSetResponse{
		Intents:  map[string]*sdcpb.TransactionSetResponseIntent{},
		Update:   []*sdcpb.Update{},
		Delete:   []*sdcpb.Path{},
		Warnings: []string{},
	}

	// process the validation results, adding them to the response
	for intentName, intentValidationResult := range validationResult {
		result.Intents[intentName] = &sdcpb.TransactionSetResponseIntent{
			Warnings: intentValidationResult.WarningsString(),
			Errors:   intentValidationResult.ErrorsString(),
		}
	}

	log.Infof("Transaction: %s - validation passed", transaction.GetTransactionId())

	// retrieve the data that is meant to be send southbound (towards the device)
	updates := root.GetHighestPrecedence(true)
	deletes, err := root.GetDeletes(true)
	if err != nil {
		return nil, err
	}

	// convert updates from cache.Update to sdcpb.Update
	// adding them to the response
	result.Update, err = cacheUpdateToSdcpbUpdate(updates)
	if err != nil {
		return nil, err
	}

	// add all the deletes to the setDataReq
	for _, u := range deletes {
		p, err := u.SdcpbPath()
		if err != nil {
			return nil, err
		}
		result.Delete = append(result.Delete, p)
	}

	// if it is a dry run, return now, skipping updating the device or the cache
	if dryRun {
		return result, nil
	}

	// apply the resulting config to the device
	dataResp, err := d.applyIntent(ctx, root)
	if err != nil {
		return nil, err
	}
	result.Warnings = append(result.Warnings, dataResp.GetWarnings()...)

	log.Infof("ds=%s transaction=%s applied", d.Name(), transaction.GetTransactionId())

	/////////////////////////////////////
	// update intent in intended store //
	/////////////////////////////////////

	// logging
	strSl := tree.Map(updates.ToCacheUpdateSlice(), func(u *cache.Update) string { return u.String() })
	log.Debugf("Updates\n%s", strings.Join(strSl, "\n"))

	delSl := deletes.PathSlices()

	log.Debugf("Deletes:\n%s", strings.Join(strSl, "\n"))

	for _, intent := range transaction.GetNewIntents() {
		// retrieve the data that is meant to be send towards the cache
		updatesOwner := root.GetUpdatesForOwner(intent.GetName())
		deletesOwner := root.GetDeletesForOwner(intent.GetName())

		// logging
		strSl := tree.Map(updatesOwner, func(u *cache.Update) string { return u.String() })
		log.Debugf("Updates Owner: %s\n%s", intent.GetName(), strings.Join(strSl, "\n"))

		delSl := deletesOwner.StringSlice()
		log.Debugf("Deletes Owner: %s \n%s", intent.GetName(), strings.Join(delSl, "\n"))

		// modify intended store per intent
		err = d.cacheClient.Modify(ctx, d.Name(), &cache.Opts{
			Store:    cachepb.Store_INTENDED,
			Owner:    intent.GetName(),
			Priority: intent.GetPriority(),
		}, deletesOwner.ToStringSlice(), updatesOwner)

		if err != nil {
			return nil, fmt.Errorf("failed updating the intended store for %s: %w", d.Name(), err)
		}
	}

	// fast and optimistic writeback to the config store
	err = d.cacheClient.Modify(ctx, d.Name(), &cache.Opts{
		Store: cachepb.Store_CONFIG,
	}, delSl.ToStringSlice(), updates.ToCacheUpdateSlice())
	if err != nil {
		return nil, fmt.Errorf("failed updating the running config store for %s: %w", d.Name(), err)
	}

	log.Infof("ds=%s transaction=%s: completed", d.Name(), transaction.GetTransactionId())
	// start the rollback ticker only if it was not already a rollback transaction.
	if !transaction.IsRollback() {
		// start the RollbackTimer
		err = transaction.StartRollbackTimer()
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (d *Datastore) TransactionSet(ctx context.Context, transactionId string, transactionIntents []*types.TransactionIntent, replaceIntent *types.TransactionIntent, transactionTimeout time.Duration, dryRun bool) (*sdcpb.TransactionSetResponse, error) {
	var err error

	// try locking the datastore if it is locked return the specific ErrDatastoreLocked error.
	if !d.dmutex.TryLock() {
		return nil, ErrDatastoreLocked
	}
	defer d.dmutex.Unlock()

	log.Infof("Transaction: %s - start", transactionId)

	// create a new Transaction with the given transaction id
	transaction := types.NewTransaction(transactionId, d.transactionManager)
	// set the timeout on the transaction
	transaction.SetTimeout(transactionTimeout)

	var transactionGuard *types.TransactionGuard

	// Try to register the Transaction in the TransactionManager only a single transaction can be register (implicitly being active)
	for {
		select {
		case <-ctx.Done():
			// Context was canceled or timed out
			log.Errorf("Transaction: %s - context canceled or timed out: %v", transactionId, ctx.Err())
			return nil, ctx.Err()
		default:
			// Start a transaction and prepare to cancel it if any error occurs
			transactionGuard, err = d.transactionManager.RegisterTransaction(ctx, transaction)
			if transactionGuard != nil {
				defer transactionGuard.Done()
				break
			}
			log.Warnf("Transaction: %s - failed to create transaction, retrying: %v", transactionId, err)
			time.Sleep(time.Millisecond * 200)
		}
		if transactionGuard != nil {
			break
		}
	}

	// add the replaceIntent to the transaction
	transaction.SetReplace(replaceIntent)

	// if replace intent is provided, kickoff the replace intent processing first
	if transaction.GetReplace() != nil {
		replaceWarn, err := d.replaceIntent(ctx, transaction)
		if err != nil {
			return nil, err
		}
		// TODO: do something with these warnings
		_ = replaceWarn
	}

	err = transaction.AddTransactionIntents(transactionIntents, types.TransactionIntentNew)
	if err != nil {
		return nil, err
	}

	response, err := d.lowlevelTransactionSet(ctx, transaction, dryRun)
	if err != nil {
		return nil, err
	}

	// Mark the transaction as successfully committed
	transactionGuard.Success()
	return response, err
}

func cacheUpdateToSdcpbUpdate(lvs tree.LeafVariantSlice) ([]*sdcpb.Update, error) {
	result := make([]*sdcpb.Update, 0, len(lvs))
	for _, lv := range lvs {
		path, err := lv.GetEntry().SdcpbPath()
		if err != nil {
			return nil, err
		}
		value, err := lv.Update.Value()
		if err != nil {
			return nil, err
		}
		upd := &sdcpb.Update{
			Path:  path,
			Value: value,
		}
		result = append(result, upd)
	}
	return result, nil
}

func (d *Datastore) TransactionConfirm(ctx context.Context, transactionId string) error {
	log.Infof("Transaction %s - Confirm", transactionId)

	if !d.dmutex.TryLock() {
		return ErrDatastoreLocked
	}
	defer d.dmutex.Unlock()
	// everything remains as is
	return d.transactionManager.Confirm(transactionId)
}

func (d *Datastore) TransactionCancel(ctx context.Context, transactionId string) error {
	log.Infof("Transaction %s - Cancel", transactionId)

	if !d.dmutex.TryLock() {
		return ErrDatastoreLocked
	}
	defer d.dmutex.Unlock()

	return d.transactionManager.Cancel(ctx, transactionId)
}

func (d *Datastore) loadIntendedStoreHighestPrio(ctx context.Context, tscc tree.TreeSchemaCacheClient, r *tree.RootEntry, pathKeySet *tree.PathSet, skipIntents []string) error {

	// Get all entries of the already existing intent
	cacheEntries := tscc.ReadCurrentUpdatesHighestPriorities(ctx, pathKeySet.GetPaths(), 2)

	flags := tree.NewUpdateInsertFlags()

	// add all the existing entries
	for _, entry := range cacheEntries {
		// we need to skip the actual owner entries
		if slices.Contains(skipIntents, entry.Owner()) {
			continue
		}
		_, err := r.AddCacheUpdateRecursive(ctx, entry, flags)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Datastore) populateTreeWithRunning(ctx context.Context, tscc tree.TreeSchemaCacheClient, r *tree.RootEntry) error {
	upds, err := tscc.ReadRunningFull(ctx)
	if err != nil {
		return err
	}

	flags := tree.NewUpdateInsertFlags()

	for _, upd := range upds {
		newUpd := cache.NewUpdate(upd.GetPath(), upd.Bytes(), tree.RunningValuesPrio, tree.RunningIntentName, 0)
		_, err := r.AddCacheUpdateRecursive(ctx, newUpd, flags)
		if err != nil {
			return err
		}
	}

	return nil
}

func pathIsKeyAsLeaf(p *sdcpb.Path) bool {
	numPElem := len(p.GetElem())
	if numPElem < 2 {
		return false
	}

	_, ok := p.GetElem()[numPElem-2].GetKey()[p.GetElem()[numPElem-1].GetName()]
	return ok
}

func (d *Datastore) readStoreKeysMeta(ctx context.Context, store cachepb.Store) (map[string]tree.UpdateSlice, error) {
	entryCh, err := d.cacheClient.GetKeys(ctx, d.config.Name, store)
	if err != nil {
		return nil, err
	}

	result := map[string]tree.UpdateSlice{}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e, ok := <-entryCh:
			if !ok {
				return result, nil
			}
			key := strings.Join(e.GetPath(), tree.KeysIndexSep)
			_, exists := result[key]
			if !exists {
				result[key] = tree.UpdateSlice{}
			}
			result[key] = append(result[key], e)
		}
	}
}
