package tree

import (
	"context"
	"fmt"

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"github.com/sdcio/yang-parser/xpath"
	"github.com/sdcio/yang-parser/xpath/xutils"
)

type yangParserEntryAdapter struct {
	e   Entry
	ctx context.Context
}

func newYangParserEntryAdapter(ctx context.Context, e Entry) *yangParserEntryAdapter {
	return &yangParserEntryAdapter{
		e:   e,
		ctx: ctx,
	}
}

func (y *yangParserEntryAdapter) Copy() xpath.Entry {
	return newYangParserEntryAdapter(y.ctx, y.e)
}

func (y *yangParserEntryAdapter) GetValue() (xpath.Datum, error) {
	if y.e.GetSchema().GetContainer() != nil {
		return xpath.NewBoolDatum(true), nil
	}

	lv, err := y.e.getHighestPrecedenceLeafValue(y.ctx)
	if err != nil {
		return nil, err
	}
	if lv == nil {
		return xpath.NewNodesetDatum([]xutils.XpathNode{}), nil
	}
	tv, err := lv.Update.Value()
	if err != nil {
		return nil, err
	}

	var result xpath.Datum
	switch tv.Value.(type) {
	case *sdcpb.TypedValue_BoolVal:
		result = xpath.NewBoolDatum(tv.GetBoolVal())
	case *sdcpb.TypedValue_StringVal:
		prefix := ""
		if y.e.GetSchema().GetField().GetType().GetTypeName() == "identityref" {
			prefix = fmt.Sprintf("%s:", y.e.GetSchema().GetField().GetType().IdentityPrefix)
		}
		result = xpath.NewLiteralDatum(prefix + tv.GetStringVal())
	case *sdcpb.TypedValue_UintVal:
		result = xpath.NewNumDatum(float64(tv.GetUintVal()))
	default:
		result = xpath.NewLiteralDatum(tv.GetStringVal())
	}
	return result, nil
}

func (y *yangParserEntryAdapter) FollowLeafRef() (xpath.Entry, error) {
	entries, err := y.e.NavigateLeafRef(y.ctx)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("error resolving leafref for %s", y.e.Path())
	}

	return newYangParserEntryAdapter(y.ctx, entries[0]), nil
}

func (y *yangParserEntryAdapter) GetPath() []string {
	return y.e.Path()
}

func (y *yangParserEntryAdapter) Navigate(p []string) (xpath.Entry, error) {
	var err error
	var rootPath = false

	if len(p) == 0 {
		return y, nil
	}

	// if the path slice starts with a / then it is a root based path.
	if p[0] == "/" {
		p = p[1:]
		rootPath = true
	}

	lookedUpEntry := y.e
	for idx, pelem := range p {
		// if we move up, on a .. we should just go up, staying in the branch that represents the instance
		// if there is another .. then we need to forward to the element with the schema and just then forward
		// to the parent. Thereby skipping the key levels that sit inbetween
		if pelem == ".." && lookedUpEntry.GetSchema().GetSchema() == nil {
			lookedUpEntry, _ = lookedUpEntry.GetFirstAncestorWithSchema()
		}

		// rootPath && idx == 0 => means only allow true on first index, for sure false on all other
		lookedUpEntry, err = lookedUpEntry.Navigate(y.ctx, []string{pelem}, rootPath && idx == 0)
		if err != nil {
			return newYangParserValueEntry(xpath.NewNodesetDatum([]xutils.XpathNode{}), err), nil
		}
	}

	return newYangParserEntryAdapter(y.ctx, lookedUpEntry), nil
}

type yangParserValueEntry struct {
	d xpath.Datum
	e error
}

func newYangParserValueEntry(d xpath.Datum, err error) *yangParserValueEntry {
	return &yangParserValueEntry{
		d: d,
		e: err,
	}
}

func (y *yangParserValueEntry) Copy() xpath.Entry {
	return y
}

func (y *yangParserValueEntry) FollowLeafRef() (xpath.Entry, error) {
	return nil, fmt.Errorf("yangParserValueEntry navigation impossible")
}

func (y *yangParserValueEntry) Navigate(p []string) (xpath.Entry, error) {
	return nil, fmt.Errorf("yangParserValueEntry navigation impossible")
}

func (y *yangParserValueEntry) GetValue() (xpath.Datum, error) {
	return y.d, nil
}

func (y *yangParserValueEntry) GetPath() []string {
	return nil
}
