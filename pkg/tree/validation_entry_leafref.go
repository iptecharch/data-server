package tree

import (
	"context"
	"fmt"
	"strings"

	"github.com/sdcio/data-server/pkg/utils"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
)

// NavigateLeafRef
func (s *sharedEntryAttributes) NavigateLeafRef(ctx context.Context) ([]Entry, error) {
	lref := s.schema.GetField().GetType().GetLeafref()
	if s.schema == nil || lref == "" {
		return nil, fmt.Errorf("error not a leafref %s", s.Path().String())
	}

	lv := s.leafVariants.GetHighestPrecedence(false)

	lref, err := utils.StripPathElemPrefix(lref)
	if err != nil {
		return nil, fmt.Errorf("failed stripping namespaces from leafref %s LeafVariant %v: %w", s.Path(), lv, err)
	}

	lrefSdcpbPath, err := utils.ParsePath(lref)
	if err != nil {
		return nil, fmt.Errorf("failed parsing leafref path %s LeafVariant %v: %w", s.Path(), lv, err)
	}

	// if the lrefs first character is "/" then it is a root based path
	isRootBasedPath := false
	if string(lref[0]) == "/" {
		isRootBasedPath = true
	}

	tv, err := lv.Update.Value()
	if err != nil {
		return nil, fmt.Errorf("failed reading value from %s LeafVariant %v: %w", s.Path(), lv, err)
	}
	value := tv.GetStringVal()

	var resultEntries []Entry

	lrefPath := newLrefPath(lrefSdcpbPath)

	var processEntries []Entry
	if isRootBasedPath {
		processEntries = []Entry{s.GetRoot()}
	} else {
		var entry Entry = s
		dotdotcount := 0
		sdcpbUp := []*sdcpb.PathElem{}
		// process the .. instructions
		for _, elem := range lrefPath {
			// if we've processed all the .., we're done here
			// break and continue
			if elem.Name != ".." {
				break
			}
			sdcpbUp = append(sdcpbUp, &sdcpb.PathElem{Name: ".."})
			dotdotcount++
		}
		// else navigate (basically up)
		entry, err = entry.NavigateSdcpbPath(ctx, sdcpbUp, false)
		if err != nil {
			return nil, err
		}
		processEntries = []Entry{entry}
		lrefPath = lrefPath[dotdotcount:]
	}

	count := 0
	// forward the path by a single element
	for _, elem := range lrefPath {

		resultEntries = []Entry{}
		err := s.resolve_leafref_key_path(ctx, elem.Keys)
		if err != nil {
			return nil, err
		}

		// we need to do the forwearding for all the already lookedup paths
		for _, entry := range processEntries {
			entry, err = entry.Navigate(ctx, []string{elem.Name}, false)
			if err != nil {
				return nil, err
			}

			// if the entry is marked for deletion, skip it
			if !entry.remainsToExist() {
				continue
			}
			// if we're at the final level, no child filtering is needed any more,
			// just return the result
			if count == len(lrefPath)-1 {
				resultEntries = append(resultEntries, entry)
				continue
			}
			var childs []Entry
			// if the entry is a list with keys, try filtering the entries based on the keys
			if len(entry.GetSchemaKeys()) > 0 {
				// filter the keys
				childs, err = entry.FilterChilds(elem.KeysToMap())
				if err != nil {
					return nil, err
				}
			} else {
				// if no keys are present,
				// add the entry to the childs list
				childs = append(childs, entry)
			}
			resultEntries = append(resultEntries, childs...)
		}
		processEntries = resultEntries
		count++
	}

	resultEntries = []Entry{}

	for _, e := range processEntries {

		r, err := e.getHighestPrecedenceLeafValue(ctx)
		if err != nil {
			return nil, err
		}
		val, err := r.Update.Value()
		if err != nil {
			return nil, err
		}
		if val.GetStringVal() == value {
			resultEntries = append(resultEntries, e)
		}
	}

	return resultEntries, nil
}

func (s *sharedEntryAttributes) resolve_leafref_key_path(ctx context.Context, keys map[string]*lrefPathElemKeyValue) error {
	// resolve keys
	for k, v := range keys {

		if v.doNotResolve || !strings.Contains(v.value, "/") || !strings.Contains(v.value, "current") {
			continue
		}
		isRootPath := true

		var keyp *sdcpb.Path
		keyp, err := utils.ParsePath(v.value)
		if err != nil {
			return err
		}

		// replace current() with its actual value
		if strings.ToLower(keyp.Elem[0].Name) == "current()" {
			keyp.Elem = keyp.Elem[1:]
			isRootPath = false
		}

		keyValue, err := s.NavigateSdcpbPath(ctx, keyp.Elem, isRootPath)
		if err != nil {
			return err
		}

		lvs := keyValue.GetHighestPrecedence(LeafVariantSlice{}, false)
		tv, err := lvs[0].Value()
		if err != nil {
			return err
		}
		keys[k].value = tv.GetStringVal()
		keys[k].doNotResolve = true
	}
	return nil
}

func (s *sharedEntryAttributes) validateLeafRefs(ctx context.Context, errchan chan<- error) {
	lref := s.schema.GetField().GetType().GetLeafref()
	if s.schema == nil || lref == "" {
		return
	}

	entry, err := s.NavigateLeafRef(ctx)
	if err != nil || len(entry) == 0 {
		errchan <- fmt.Errorf("missing leaf reference: failed resolving leafref %s for %s: %v", lref, s.Path().String(), err)
		return
	}

	// Only if the value remains, even after the SetIntent made it through, the LeafRef can be considered resolved.
	if !entry[0].remainsToExist() {
		lv := s.leafVariants.GetHighestPrecedence(false)
		EntryPath, _ := s.SdcpbPath()
		errchan <- fmt.Errorf("missing leaf reference: failed resolving leafref %s for %s to path %s LeafVariant %v", lref, utils.ToXPath(EntryPath, false), s.Path().String(), lv)
		return
	}
}

// lrefPath for the leafref resolution we need to distinguish between already resolved values and not yet resolved xpath statements
// this is a struct that provides this information
type lrefPath []*lrefPathElem

type lrefPathElem struct {
	Name string
	Keys map[string]*lrefPathElemKeyValue
}

func (l *lrefPathElem) KeysToMap() map[string]string {
	result := map[string]string{}
	for k, v := range l.Keys {
		result[k] = v.value
	}
	return result
}

type lrefPathElemKeyValue struct {
	doNotResolve bool
	value        string
}

func newLrefPath(p *sdcpb.Path) lrefPath {
	lp := lrefPath{}
	for _, x := range p.Elem {
		lrefpe := &lrefPathElem{
			Name: x.Name,
			Keys: map[string]*lrefPathElemKeyValue{},
		}
		for k, v := range x.Key {
			lrefpe.Keys[k] = &lrefPathElemKeyValue{
				value:        v,
				doNotResolve: false,
			}
		}
		lp = append(lp, lrefpe)
	}

	return lp

}

func (pes lrefPath) ToSdcpbPathElem() []*sdcpb.PathElem {
	result := make([]*sdcpb.PathElem, 0, len(pes))
	for _, e := range pes {
		pe := &sdcpb.PathElem{
			Name: e.Name,
			Key:  map[string]string{},
		}
		for k, v := range e.Keys {
			pe.Key[k] = v.value
		}
		result = append(result, pe)
	}
	return result
}
