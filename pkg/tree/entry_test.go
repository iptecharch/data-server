package tree

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/utils/testhelper"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

var (
	flagsNew      *UpdateInsertFlags
	flagsExisting *UpdateInsertFlags
)

func init() {
	flagsNew = NewUpdateInsertFlags()
	flagsNew.SetNewFlag()
	flagsExisting = NewUpdateInsertFlags()
}

func Test_Entry(t *testing.T) {

	desc, err := proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_StringVal{StringVal: "MyDescription"}})
	if err != nil {
		t.Error(err)
	}

	u1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "9", "description"}, desc, int32(100), "me", int64(9999999))
	u2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc, int32(99), "me", int64(444))
	u3 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc, int32(98), "me", int64(88))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Error(err)
	}

	for _, u := range []*cache.Update{u1, u2, u3} {
		_, err = root.AddCacheUpdateRecursive(ctx, u, flagsNew)
		if err != nil {
			t.Error(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	r := []string{}
	r = root.StringIndent(r)
	t.Log(strings.Join(r, "\n"))
}

func Test_Entry_One(t *testing.T) {
	desc1 := testhelper.GetStringTvProto(t, "DescriptionOne")
	desc2 := testhelper.GetStringTvProto(t, "DescriptionTwo")
	desc3 := testhelper.GetStringTvProto(t, "DescriptionThree")

	prio100 := int32(100)
	prio50 := int32(50)

	owner1 := "OwnerOne"
	owner2 := "OwnerTwo"

	ts1 := int64(9999999)

	u1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "9", "description"}, desc1, prio100, owner1, ts1)
	u2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc2, prio100, owner1, ts1)
	u3 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc3, prio50, owner2, ts1)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Error(err)
	}

	// start test
	for _, u := range []*cache.Update{u1, u2, u3} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
		if err != nil {
			t.Error(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	// log the tree
	t.Log(root.String())

	t.Run("Test 1 - expect 2 entry for owner1", func(t *testing.T) {
		o1Le := []*LeafEntry{}
		o1Le = root.GetByOwner(owner1, o1Le)
		o1 := LeafEntriesToCacheUpdates(o1Le)
		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{u2, u1}, o1); diff != "" {
			t.Errorf("root.GetByOwner(owner1) mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Test 2 - expect 1 entry for owner2", func(t *testing.T) {
		o2Le := []*LeafEntry{}
		o2Le = root.GetByOwner(owner2, o2Le)
		o2 := LeafEntriesToCacheUpdates(o2Le)
		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{u3}, o2); diff != "" {
			t.Errorf("root.GetByOwner(owner2) mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Test 3 - GetHighesPrio()", func(t *testing.T) {

		highprec := root.GetHighestPrecedence(true)
		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{u1, u3}, highprec.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})
}

// Test_Entry_Two adding a new Update with same owner and priority but updating the value
func Test_Entry_Two(t *testing.T) {

	desc3 := testhelper.GetStringTvProto(t, "DescriptionThree")
	prio50 := int32(50)
	owner1 := "OwnerOne"
	ts1 := int64(9999999)
	u1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc3, prio50, owner1, ts1)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Error(err)
	}

	// start test add "existing" data
	for _, u := range []*cache.Update{u1} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
		if err != nil {
			t.Error(err)
		}
	}

	// add incomming set intent reques data
	overwriteDesc := testhelper.GetStringTvProto(t, "Owerwrite Description")

	// adding a new Update with same owner and priority with different value
	n1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, overwriteDesc, prio50, owner1, ts1)

	for _, u := range []*cache.Update{n1} {
		_, err = root.AddCacheUpdateRecursive(ctx, u, flagsNew)
		if err != nil {
			t.Error(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	// log the tree
	t.Log(root.String())
	highprec := root.GetHighestPrecedence(true)

	// diff the result with the expected
	if diff := testhelper.DiffCacheUpdates([]*cache.Update{n1}, highprec.ToCacheUpdateSlice()); diff != "" {
		t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
	}
}

// Test_Entry_Three Checks that an Intent update is processed properly
func Test_Entry_Three(t *testing.T) {
	desc3 := testhelper.GetStringTvProto(t, "DescriptionThree")
	prio50 := int32(50)
	owner1 := "OwnerOne"
	ts1 := int64(9999999)
	u1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc3, prio50, owner1, ts1)
	u2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "11", "description"}, desc3, prio50, owner1, ts1)
	u3 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "12", "description"}, desc3, prio50, owner1, ts1)
	u4 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "13", "description"}, desc3, prio50, owner1, ts1)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Error(err)
	}

	// start test add "existing" data
	for _, u := range []*cache.Update{u1, u2, u3, u4} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
		if err != nil {
			t.Error(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	t.Run("Check the data is present", func(t *testing.T) {

		// log the tree
		t.Log(root.String())

		highpri := root.GetHighestPrecedence(false)

		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{u1, u2, u3, u4}, highpri.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Check non is reported as New or Updated", func(t *testing.T) {

		// log the tree
		t.Log(root.String())

		highpri := root.GetHighestPrecedence(true)

		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{}, highpri.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})

	// indicate that the intent is receiving an update
	// therefor invalidate all the present entries of the owner / intent
	root.markOwnerDelete(owner1, false)

	// add incomming set intent reques data
	overwriteDesc := testhelper.GetStringTvProto(t, "Owerwrite Description")

	// adding a new Update with same owner and priority with different value
	n1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, overwriteDesc, prio50, owner1, ts1)
	n2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "11", "description"}, overwriteDesc, prio50, owner1, ts1)

	for _, u := range []*cache.Update{n1, n2} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
		if err != nil {
			t.Error(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	// log the tree
	t.Log(root.String())

	t.Run("Check the original intent data of owner 1 is gone", func(t *testing.T) {

		// log the tree
		t.Log(root.String())

		highPriLe := root.getByOwnerFiltered(owner1, FilterNonDeleted)

		highPri := LeafEntriesToCacheUpdates(highPriLe)

		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{n1, n2}, highPri); diff != "" {
			t.Errorf("root.GetByOwner() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Check the old entries are gone", func(t *testing.T) {
		highpri := root.GetHighestPrecedence(true)
		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{n1, n2}, highpri.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})

}

// Test_Entry_Four Checks that an Intent update is processed properly with an intent that is shadowed initially.
func Test_Entry_Four(t *testing.T) {
	desc3 := testhelper.GetStringTvProto(t, "DescriptionThree")
	prio50 := int32(50)
	prio55 := int32(55)
	owner1 := "OwnerOne"
	owner2 := "OwnerTwo"
	ts1 := int64(9999999)

	u1o1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc3, prio50, owner1, ts1)
	u2o1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "11", "description"}, desc3, prio50, owner1, ts1)
	u3 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "12", "description"}, desc3, prio50, owner1, ts1)
	u4 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "13", "description"}, desc3, prio50, owner1, ts1)

	u1o2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "10", "description"}, desc3, prio55, owner2, ts1)
	u2o2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "11", "description"}, desc3, prio55, owner2, ts1)

	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Error(err)
	}

	// start test add "existing" data
	for _, u := range []*cache.Update{u1o1, u2o1, u3, u4, u1o2, u2o2} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
		if err != nil {
			t.Error(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	t.Run("Check the data is present", func(t *testing.T) {

		// log the tree
		t.Log(root.String())

		highprec := root.GetHighestPrecedence(false)

		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{u1o1, u2o1, u3, u4}, highprec.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})

	// indicate that the intent is receiving an update
	// therefor invalidate all the present entries of the owner / intent
	root.markOwnerDelete(owner1, false)

	// add incomming set intent reques data
	overwriteDesc := testhelper.GetStringTvProto(t, "Owerwrite Description")

	// adding a new Update with same owner and priority with different value
	n1 := cache.NewUpdate([]string{"interface", "ethernet-0/1", "subinterface", "10", "description"}, overwriteDesc, prio50, owner1, ts1)
	n2 := cache.NewUpdate([]string{"interface", "ethernet-0/1", "subinterface", "11", "description"}, overwriteDesc, prio50, owner1, ts1)

	for _, u := range []*cache.Update{n1, n2} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
		if err != nil {
			t.Error(err)
		}
	}
	root.FinishInsertionPhase(ctx)

	// log the tree
	t.Log(root.String())

	t.Run("Check the data is gone from ByOwner with NonDelete Filter", func(t *testing.T) {

		// log the tree
		t.Log(root.String())

		highPriLe := root.getByOwnerFiltered(owner1, FilterNonDeleted)

		highPri := LeafEntriesToCacheUpdates(highPriLe)

		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{n1, n2}, highPri); diff != "" {
			t.Errorf("root.GetByOwner() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Check the old entries are gone from highest", func(t *testing.T) {
		highpri := root.GetHighestPrecedence(true)
		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{n1, n2, u1o2, u2o2}, highpri.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Check the old entries are gone from highest (only New Or Updated)", func(t *testing.T) {
		highpri := root.GetHighestPrecedence(true)
		// diff the result with the expected
		if diff := testhelper.DiffCacheUpdates([]*cache.Update{u1o2, u2o2, n1, n2}, highpri.ToCacheUpdateSlice()); diff != "" {
			t.Errorf("root.GetHighesPrio() mismatch (-want +got):\n%s", diff)
		}
	})
}
func Test_Validation_Leaflist_Min_Max(t *testing.T) {
	prio50 := int32(50)
	owner1 := "OwnerOne"
	ts1 := int64(9999999)

	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test Leaflist min- & max- elements - One",
		func(t *testing.T) {

			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), owner1)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}

			leaflistval := testhelper.GetLeafListTvProto(t,
				[]*sdcpb.TypedValue{
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data"},
					},
				},
			)

			u1 := cache.NewUpdate([]string{"leaflist", "entry"}, leaflistval, prio50, owner1, ts1)

			// start test add "existing" data
			for _, u := range []*cache.Update{u1} {
				_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
				if err != nil {
					t.Fatal(err)
				}
			}

			root.FinishInsertionPhase(ctx)

			validationResult := root.Validate(context.TODO(), false)

			// check if errors are received
			// If so, join them and return the cumulated errors
			errs := validationResult.ErrorsStr()
			if len(errs) != 1 {
				t.Errorf("expected 1 error but got %d, %v", len(errs), errs)
			}
		},
	)

	t.Run("Test Leaflist min- & max- elements - Two",
		func(t *testing.T) {
			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), owner1)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}

			leaflistval := testhelper.GetLeafListTvProto(t,
				[]*sdcpb.TypedValue{
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data1"},
					},
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data2"},
					},
				},
			)

			u1 := cache.NewUpdate([]string{"leaflist", "entry"}, leaflistval, prio50, owner1, ts1)

			// start test add "existing" data
			for _, u := range []*cache.Update{u1} {
				_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
				if err != nil {
					t.Fatal(err)
				}
			}

			validationResult := root.Validate(context.TODO(), false)

			// check if errors are received
			// If so, join them and return the cumulated errors
			errs := validationResult.ErrorsStr()
			if len(errs) != 0 {
				t.Errorf("expected 0 errors but got %d, %v", len(errs), errs)
			}
		},
	)
	t.Run("Test Leaflist min- & max- elements - Four",
		func(t *testing.T) {

			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), owner1)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}

			leaflistval := testhelper.GetLeafListTvProto(t,
				[]*sdcpb.TypedValue{
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data1"},
					},
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data2"},
					},
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data3"},
					},
					{
						Value: &sdcpb.TypedValue_StringVal{StringVal: "data4"},
					},
				},
			)

			u1 := cache.NewUpdate([]string{"leaflist", "entry"}, leaflistval, prio50, owner1, ts1)

			// start test add "existing" data
			for _, u := range []*cache.Update{u1} {
				_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
				if err != nil {
					t.Fatal(err)
				}
			}

			validationResult := root.Validate(context.TODO(), false)

			// check if errors are received
			// If so, join them and return the cumulated errors
			errs := validationResult.ErrorsStr()
			if len(errs) != 1 {
				t.Errorf("expected 1 error but got %d, %v", len(errs), errs)
			}
		},
	)
}

func Test_Entry_Delete_Aggregation(t *testing.T) {
	desc3 := testhelper.GetStringTvProto(t, "DescriptionThree")
	prio50 := int32(50)
	owner1 := "OwnerOne"
	ts1 := int64(9999999)

	u1 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "description"}, desc3, prio50, owner1, ts1)
	u2 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "name"}, testhelper.GetStringTvProto(t, "ethernet-0/0"), prio50, owner1, ts1)
	u3 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "0", "index"}, testhelper.GetStringTvProto(t, "0"), prio50, owner1, ts1)
	u4 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "0", "description"}, desc3, prio50, owner1, ts1)
	u5 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "1", "index"}, testhelper.GetStringTvProto(t, "1"), prio50, owner1, ts1)
	u6 := cache.NewUpdate([]string{"interface", "ethernet-0/0", "subinterface", "1", "description"}, desc3, prio50, owner1, ts1)

	ctx := context.TODO()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Fatal(err)
	}

	// start test add "existing" data
	for _, u := range []*cache.Update{u1, u2, u3, u4, u5, u6} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsExisting)
		if err != nil {
			t.Fatal(err)
		}
	}

	// get ready to add the new intent data
	root.markOwnerDelete(owner1, false)

	u1n := cache.NewUpdate([]string{"interface", "ethernet-0/1", "description"}, desc3, prio50, owner1, ts1)
	u2n := cache.NewUpdate([]string{"interface", "ethernet-0/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-0/1"), prio50, owner1, ts1)

	// start test add "new" / request data
	for _, u := range []*cache.Update{u1n, u2n} {
		_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
		if err != nil {
			t.Fatal(err)
		}
	}

	root.FinishInsertionPhase(ctx)

	// retrieve the Deletes
	deletesSlices, err := root.GetDeletes(true)
	if err != nil {
		t.Error(err)
	}

	// process the result for comparison
	deletes := make([]string, 0, len(deletesSlices))
	for _, x := range deletesSlices {
		deletes = append(deletes, strings.Join(x.Path(), "/"))
	}

	// define the expected result
	expects := []string{
		"interface/ethernet-0/0",
	}
	// sort both slices for equality check
	slices.Sort(deletes)
	slices.Sort(expects)

	// perform comparison
	if diff := cmp.Diff(expects, deletes); diff != "" {
		t.Errorf("root.GetDeletes() mismatch (-want +got):\n%s", diff)
	}
}

// TestLeafVariants_GetHighesPrio
func TestLeafVariants_GetHighesPrio(t *testing.T) {
	owner1 := "owner1"
	owner2 := "owner2"
	ts := int64(0)
	path := []string{"firstPathElem"}

	// test that if highes prio is to be deleted, that the second highes is returned,
	// because thats an update.
	t.Run("Delete Non New",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})

			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 2, owner1, ts), flagsExisting, nil))
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 1, owner2, ts), flagsExisting, nil))
			lv.les[1].MarkDelete(false)

			le := lv.GetHighestPrecedence(true, false)

			if le != lv.les[0] {
				t.Errorf("expected to get entry %v, got %v", lv.les[0], le)
			}

		},
	)

	// Have only a single entry in the list, thats marked for deletion
	// should return nil
	t.Run("Single entry thats also marked for deletion",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 1, owner1, ts), flagsExisting, nil))
			lv.les[0].MarkDelete(false)

			le := lv.GetHighestPrecedence(true, false)

			if le != nil {
				t.Errorf("expected to get entry %v, got %v", nil, le)
			}
		},
	)

	// A preferred entry does exist the lower prio entry is updated.
	// on onlyIfPrioChanged == true we do not expect output.
	t.Run("New Low Prio IsUpdate OnlyChanged True",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 5, owner1, ts), flagsExisting, nil))
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 6, owner2, ts), flagsNew, nil))

			le := lv.GetHighestPrecedence(true, false)

			if le != nil {
				t.Errorf("expected to get entry %v, got %v", nil, le)
			}
		},
	)

	// A preferred entry does exist the lower prio entry is updated.
	// on onlyIfPrioChanged == false we do not expect the highes prio update to be returned.
	t.Run("New Low Prio IsUpdate OnlyChanged False",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 5, owner1, ts), flagsExisting, nil))
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 6, owner1, ts), flagsNew, nil))

			le := lv.GetHighestPrecedence(false, false)

			if le != lv.les[0] {
				t.Errorf("expected to get entry %v, got %v", lv.les[0], le)
			}
		},
	)

	// A preferred entry does exist the lower prio entry is new.
	// // on onlyIfPrioChanged == true we do not expect output.
	t.Run("New Low Prio IsNew OnlyChanged == True",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})

			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 5, owner1, ts), flagsExisting, nil))
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 6, owner2, ts), flagsNew, nil))

			le := lv.GetHighestPrecedence(true, false)

			if le != nil {
				t.Errorf("expected to get entry %v, got %v", nil, le)
			}
		},
	)

	t.Run("New Low Prio IsNew OnlyChanged == False",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 5, owner1, ts), flagsExisting, nil))
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 6, owner2, ts), flagsNew, nil))

			le := lv.GetHighestPrecedence(false, false)

			if le != lv.les[0] {
				t.Errorf("expected to get entry %v, got %v", lv.les[0], le)
			}
		},
	)

	// If no entries exist in the list nil should be returned.
	t.Run("No Entries",
		func(t *testing.T) {
			lv := LeafVariants{}

			le := lv.GetHighestPrecedence(true, false)

			if le != nil {
				t.Errorf("expected to get entry %v, got %v", nil, le)
			}
		},
	)

	// make sure the secondhighes is also populated if the highes was the first entry
	t.Run("secondhighes populated if highes was first",
		func(t *testing.T) {
			lv := newLeafVariants(&TreeContext{})
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 1, owner1, ts), flagsExisting, nil))
			lv.les[0].MarkDelete(false)
			lv.Add(NewLeafEntry(cache.NewUpdate(path, nil, 2, owner2, ts), flagsExisting, nil))

			le := lv.GetHighestPrecedence(true, false)

			if le != lv.les[1] {
				t.Errorf("expected to get entry %v, got %v", lv.les[1], le)
			}
		},
	)
}

func expectNil(t *testing.T, a any, name string) {
	fail := true
	switch reflect.TypeOf(a).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		fail = !reflect.ValueOf(a).IsNil()
	default:
		if a != nil {
			fail = true
		}
	}

	if fail {
		t.Errorf("expected %s to be nil, but is %v", name, a)
	}
}

func expectNotNil(t *testing.T, a any, name string) {
	fail := true
	switch reflect.TypeOf(a).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		fail = reflect.ValueOf(a).IsNil()
	default:
		if a != nil {
			fail = false
		}
	}

	if fail {
		t.Errorf("expected %s to not be nil", name)
	}
}

func Test_Schema_Population(t *testing.T) {

	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Fatal(err)
	}

	interf, err := newSharedEntryAttributes(ctx, root.sharedEntryAttributes, "interface", tc)
	if err != nil {
		t.Error(err)
	}
	expectNotNil(t, interf.schema, "/interface schema")

	e00, err := newSharedEntryAttributes(ctx, interf, "ethernet-0/0", tc)
	if err != nil {
		t.Error(err)
	}
	expectNil(t, e00.schema, "/interface/ethernet-0/0 schema")

	dk, err := newSharedEntryAttributes(ctx, root.sharedEntryAttributes, "doublekey", tc)
	if err != nil {
		t.Error(err)
	}
	expectNotNil(t, dk.schema, "/doublekey schema")

	dkk1, err := newSharedEntryAttributes(ctx, dk, "key1", tc)
	if err != nil {
		t.Error(err)
	}
	expectNil(t, dkk1.schema, "/doublekey/key1 schema")

	dkk2, err := newSharedEntryAttributes(ctx, dkk1, "key2", tc)
	if err != nil {
		t.Error(err)
	}
	expectNil(t, dkk2.schema, "/doublekey/key2 schema")

	dkkv, err := newSharedEntryAttributes(ctx, dkk2, "mandato", tc)
	if err != nil {
		t.Error(err)
	}
	expectNotNil(t, dkkv.schema, "/doublekey[key1,key2]mandato schema")
}

func Test_sharedEntryAttributes_SdcpbPath(t *testing.T) {
	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Fatal(err)
	}

	interf, err := newSharedEntryAttributes(ctx, root.sharedEntryAttributes, "interface", tc)
	if err != nil {
		t.Error(err)
	}

	e00, err := newSharedEntryAttributes(ctx, interf, "ethernet-0/0", tc)
	if err != nil {
		t.Error(err)
	}

	e00desc, err := newSharedEntryAttributes(ctx, e00, "description", tc)
	if err != nil {
		t.Error(err)
	}

	dk, err := newSharedEntryAttributes(ctx, root.sharedEntryAttributes, "doublekey", tc)
	if err != nil {
		t.Error(err)
	}

	dkk1, err := newSharedEntryAttributes(ctx, dk, "key1", tc)
	if err != nil {
		t.Error(err)
	}

	dkk2, err := newSharedEntryAttributes(ctx, dkk1, "key2", tc)
	if err != nil {
		t.Error(err)
	}

	dkkv, err := newSharedEntryAttributes(ctx, dkk2, "mandato", tc)
	if err != nil {
		t.Error(err)
	}

	t.Run("singlekey",
		func(t *testing.T) {
			p, err := e00.SdcpbPath()
			if err != nil {
				t.Error(err)
			}
			cmpPath := &sdcpb.Path{
				Elem: []*sdcpb.PathElem{
					{
						Name: "interface",
						Key: map[string]string{
							"name": "ethernet-0/0",
						},
					},
				},
			}

			cmp.Diff(p.String(), cmpPath.String)
		},
	)

	t.Run("singlekey - leaf",
		func(t *testing.T) {
			p, err := e00desc.SdcpbPath()
			if err != nil {
				t.Error(err)
			}
			cmpPath := &sdcpb.Path{
				Elem: []*sdcpb.PathElem{
					{
						Name: "interface",
						Key: map[string]string{
							"name": "ethernet-0/0",
						},
					},
					{
						Name: "description",
					},
				},
			}

			cmp.Diff(p.String(), cmpPath.String)
		},
	)

	t.Run("doublekey",
		func(t *testing.T) {

			p, err := dkkv.SdcpbPath()
			if err != nil {
				t.Error(err)
			}

			cmpPath := &sdcpb.Path{
				Elem: []*sdcpb.PathElem{
					{
						Name: "doublekey",
						Key: map[string]string{
							"key1": "key1",
							"key2": "key2",
						},
					},
					{
						Name: "mandato",
					},
				},
			}

			cmp.Diff(p.String(), cmpPath.String)
		},
	)

}

func Test_sharedEntryAttributes_getKeyName(t *testing.T) {
	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), "foo")

	root, err := NewTreeRoot(ctx, tc)
	if err != nil {
		t.Fatal(err)
	}

	interf, err := newSharedEntryAttributes(ctx, root.sharedEntryAttributes, "interface", tc)
	if err != nil {
		t.Error(err)
	}

	e00, err := newSharedEntryAttributes(ctx, interf, "ethernet-0/0", tc)
	if err != nil {
		t.Error(err)
	}

	e00desc, err := newSharedEntryAttributes(ctx, e00, "description", tc)
	if err != nil {
		t.Error(err)
	}

	dk, err := newSharedEntryAttributes(ctx, root.sharedEntryAttributes, "doublekey", tc)
	if err != nil {
		t.Error(err)
	}

	dkk1, err := newSharedEntryAttributes(ctx, dk, "key1", tc)
	if err != nil {
		t.Error(err)
	}

	dkk2, err := newSharedEntryAttributes(ctx, dkk1, "key2", tc)
	if err != nil {
		t.Error(err)
	}

	dkkv, err := newSharedEntryAttributes(ctx, dkk2, "mandato", tc)
	if err != nil {
		t.Error(err)
	}

	t.Run("double key", func(t *testing.T) {
		_, err := dkkv.getKeyName()
		if err == nil {
			t.Errorf("expected an error when getKeyName on %s", strings.Join(dkkv.Path(), " "))
		}
	})

	t.Run("double key - key1", func(t *testing.T) {
		s, err := dkk1.getKeyName()
		if err != nil {
			t.Errorf("expected no error getKeyName on %s", strings.Join(dkkv.Path(), " "))
		}
		expected := "key1"
		if s != expected {
			t.Errorf("expected %s key value to be %s but got %s", strings.Join(dkkv.Path(), " "), expected, s)
		}
	})

	t.Run("double key - key2", func(t *testing.T) {
		s, err := dkk2.getKeyName()
		if err != nil {
			t.Errorf("expected no error getKeyName on %s", strings.Join(dkkv.Path(), " "))
		}
		expected := "key2"
		if s != expected {
			t.Errorf("expected %s key value to be %s but got %s", strings.Join(dkkv.Path(), " "), expected, s)
		}
	})

	t.Run("Leaf", func(t *testing.T) {
		_, err := e00desc.getKeyName()
		if err == nil {
			t.Errorf("expected an error when getKeyName on %s", strings.Join(e00desc.Path(), " "))
		}
	})

}

func Test_Validation_String_Pattern(t *testing.T) {
	prio50 := int32(50)
	owner1 := "OwnerOne"
	ts1 := int64(9999999)

	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test_Validation_String_Pattern - One",
		func(t *testing.T) {
			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), owner1)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}

			leafval := testhelper.GetStringTvProto(t, "data123")

			u1 := cache.NewUpdate([]string{"patterntest"}, leafval, prio50, owner1, ts1)

			for _, u := range []*cache.Update{u1} {
				_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
				if err != nil {
					t.Fatal(err)
				}
			}

			root.FinishInsertionPhase(ctx)

			validationResult := root.Validate(context.TODO(), false)

			// check if errors are received
			// If so, join them and return the cumulated errors
			if !validationResult.HasErrors() {
				errs := validationResult.ErrorsStr()
				t.Errorf("expected 1 error but got %d, %v", len(errs), errs)
			}
		},
	)
	flagsNew := NewUpdateInsertFlags()
	flagsNew.SetNewFlag()

	t.Run("Test_Validation_String_Pattern - Two",
		func(t *testing.T) {
			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), owner1)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}

			leafval := testhelper.GetStringTvProto(t, "hallo F")

			u1 := cache.NewUpdate([]string{"patterntest"}, leafval, prio50, owner1, ts1)

			for _, u := range []*cache.Update{u1} {
				_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
				if err != nil {
					t.Fatal(err)
				}
			}

			root.FinishInsertionPhase(ctx)

			validationResult := root.Validate(context.TODO(), false)

			// check if errors are received
			// If so, join them and return the cumulated errors
			if validationResult.HasErrors() {
				errs := validationResult.ErrorsStr()
				t.Errorf("expected 0 errors but got %d, %v", len(errs), errs)
			}
		},
	)

	// t.Run("Test_Validation_String_Pattern - Test Invert",
	// 	func(t *testing.T) {

	// 		root, err := NewTreeRoot(ctx, tc)
	// 		if err != nil {
	// 			t.Fatal(err)
	// 		}

	// 		leafval := testhelper.GetStringTvProto(t, "hallo DU")

	// 		u1 := cache.NewUpdate([]string{"patterntest"}, leafval, prio50, owner1, ts1)

	// 		for _, u := range []*cache.Update{u1} {
	// 			err := root.AddCacheUpdateRecursive(ctx, u, true)
	// 			if err != nil {
	// 				t.Fatal(err)
	// 			}
	// 		}

	// 		root.FinishInsertionPhase()

	// 		validationErrors := []error{}
	// 		validationErrChan := make(chan error)
	// 		go func() {
	// 			root.Validate(validationErrChan)
	// 			close(validationErrChan)
	// 		}()

	// 		// read from the Error channel
	// 		for e := range validationErrChan {
	// 			validationErrors = append(validationErrors, e)
	// 		}

	// 		// check if errors are received
	// 		// If so, join them and return the cumulated errors
	// 		if len(validationErrors) != 1 {
	// 			t.Errorf("expected 1 error but got %d, %v", len(validationErrors), validationErrors)
	// 		}
	// 	},
	// )

}

func Test_Validation_Deref(t *testing.T) {
	prio50 := int32(50)
	owner1 := "OwnerOne"
	ts1 := int64(9999999)

	ctx := context.TODO()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
	if err != nil {
		t.Fatal(err)
	}

	flagsNew := NewUpdateInsertFlags()
	flagsNew.SetNewFlag()

	t.Run("Test_Validation_String_Pattern - One",
		func(t *testing.T) {
			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", nil, scb), owner1)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}

			leafval := testhelper.GetStringTvProto(t, "data123")

			u1 := cache.NewUpdate([]string{"patterntest"}, leafval, prio50, owner1, ts1)

			for _, u := range []*cache.Update{u1} {
				_, err := root.AddCacheUpdateRecursive(ctx, u, flagsNew)
				if err != nil {
					t.Fatal(err)
				}
			}

			root.FinishInsertionPhase(ctx)

			validationResult := root.Validate(context.TODO(), false)

			// check if errors are received
			// If so, join them and return the cumulated errors
			errs := validationResult.ErrorsStr()
			if len(errs) != 1 {
				t.Errorf("expected 1 error but got %d, %v", len(errs), errs)
			}
		},
	)
}
