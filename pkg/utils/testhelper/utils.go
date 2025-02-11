package testhelper

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/sdcio/data-server/mocks/mockschemaclientbound"
	"github.com/sdcio/data-server/pkg/cache"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

const (
	pathSep = "/"
)

// diffCacheUpdates takes two []*cache.Update and compares the diff
func DiffCacheUpdates(a, b []*cache.Update) string {
	return cmp.Diff(CacheUpdateSliceToStringSlice(a), CacheUpdateSliceToStringSlice(b))
}

// CacheUpdateSliceToStringSlice converts a []*cache.Update to []string
func CacheUpdateSliceToStringSlice(s []*cache.Update) []string {
	result := make([]string, 0, len(s))
	for _, e := range s {
		result = append(result, fmt.Sprintf("%v", e))
	}
	// sort the result lexically
	slices.Sort(result)
	return result
}

// GetStringTvProto takes a string and returns the sdcpb.TypedValue for it in proto encoding as []byte
func GetStringTvProto(t *testing.T, s string) []byte {
	result, err := proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_StringVal{StringVal: s}})
	if err != nil {
		t.Error(err)
	}
	return result
}

func GetLeafListTvProto(t *testing.T, tvs []*sdcpb.TypedValue) []byte {
	result, err := proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_LeaflistVal{LeaflistVal: &sdcpb.ScalarArray{Element: tvs}}})
	if err != nil {
		t.Error(err)
	}
	return result
}

// GetStringTvProto takes a string and returns the sdcpb.TypedValue for it in proto encoding as []byte
func GetUIntTvProto(t *testing.T, i uint64) []byte {
	result, err := proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_UintVal{UintVal: uint64(i)}})
	if err != nil {
		t.Error(err)
	}
	return result
}

// PathMapIndex calculates a common map index for string slice based paths
func PathMapIndex(elems []string) string {
	return strings.Join(elems, pathSep)
}

// DiffStringSlice compares two string slices returning the
func DiffStringSlice(s1, s2 []string, forceNoSideEffect bool) string {
	var tmp []string
	// to avoid side effects we copy the slices before sorting them
	if forceNoSideEffect {
		copy(s1, tmp)
		s1 = tmp
		copy(s2, tmp)
		s2 = tmp
	}

	slices.Sort(s1)
	slices.Sort(s2)
	return cmp.Diff(s1, s2)
}

func DiffDoubleStringPathSlice(s1, s2 [][]string) string {
	s1StringSlice := make([]string, 0, len(s1))
	s2StringSlice := make([]string, 0, len(s2))

	y := []struct {
		Double [][]string
		Single []string
	}{
		{
			Double: s1,
			Single: s1StringSlice,
		},
		{
			Double: s2,
			Single: s2StringSlice,
		},
	}

	for idx, x := range y {
		for _, entry := range x.Double {
			y[idx].Single = append(y[idx].Single, PathMapIndex(entry))
		}
	}
	return DiffStringSlice(y[0].Single, y[1].Single, false)
}

// GetSchemaClientBound creates a SchemaClientBound mock that responds to certain GetSchema requests
func GetSchemaClientBound(t *testing.T, mockCtrl *gomock.Controller) (*mockschemaclientbound.MockSchemaClientBound, error) {

	x, schema, err := InitSDCIOSchema()
	if err != nil {
		return nil, err
	}

	sdcpbSchema := &sdcpb.Schema{
		Name:    schema.Name,
		Vendor:  schema.Vendor,
		Version: schema.Version,
	}

	mockscb := mockschemaclientbound.NewMockSchemaClientBound(mockCtrl)

	// make the mock respond to GetSchema requests
	mockscb.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
			return x.GetSchema(ctx, &sdcpb.GetSchemaRequest{
				Path:   path,
				Schema: sdcpbSchema,
			})
		},
	)

	// setup the ToPath() responses
	mockscb.EXPECT().ToPath(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, path []string) (*sdcpb.Path, error) {
			pr, err := x.ToPath(ctx, &sdcpb.ToPathRequest{
				PathElement: path,
				Schema:      sdcpbSchema,
			})
			if err != nil {
				return nil, err
			}
			return pr.GetPath(), nil
		},
	)

	// return the mock
	return mockscb, nil
}
