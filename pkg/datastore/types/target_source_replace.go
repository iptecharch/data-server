package types

import (
	"context"

	"github.com/beevik/etree"
	"github.com/sdcio/data-server/pkg/datastore/target"
	"github.com/sdcio/data-server/pkg/utils"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
)

// TargetSourceReplace takes a TargetSource and proxies the calls.
// Calls to ToProtoDeletes(...) [used with gnmi proto, json & json_ietf encodings]
// and ToXML(...) used in case of netconf are altered.
//   - ToProtoDeletes(...) returns only the root path.
//   - ToXML(...) returns the TargetSource generated etree.Document, but sets the
//     replace flag on the root element
type TargetSourceReplace struct {
	target.TargetSource
}

// NewTargetSourceReplace constructor for TargetSourceReplace
func NewTargetSourceReplace(ts target.TargetSource) *TargetSourceReplace {
	return &TargetSourceReplace{
		ts,
	}
}

// ToProtoDeletes In the Replace case, we need to delete from the Root down. So the call is
// not forwarded to the TargetSource but the root path is returned
func (t *TargetSourceReplace) ToProtoDeletes(ctx context.Context) ([]*sdcpb.Path, error) {
	return []*sdcpb.Path{
		{
			Elem: []*sdcpb.PathElem{},
		},
	}, nil
}

// ToXML in the XML case, we need to add the XMLReplace operation to the root element
// So the call is forwarded to the original TargetSource, the attribute is added and returned to the caller
func (t *TargetSourceReplace) ToXML(onlyNewOrUpdated bool, honorNamespace bool, operationWithNamespace bool, useOperationRemove bool) (*etree.Document, error) {
	// forward call to original TargetSource
	et, err := t.TargetSource.ToXML(onlyNewOrUpdated, honorNamespace, operationWithNamespace, useOperationRemove)
	if err != nil {
		return nil, err
	}
	// Add replace operation to the root element
	utils.AddXMLOperation(&et.Element, utils.XMLOperationReplace, operationWithNamespace, useOperationRemove)
	return et, nil
}
