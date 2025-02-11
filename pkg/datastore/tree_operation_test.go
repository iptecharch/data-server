// Copyright 2024 Nokia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/openconfig/ygot/ygot"
	"github.com/sdcio/data-server/mocks/mockcacheclient"
	"github.com/sdcio/data-server/mocks/mocktarget"
	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/config"
	"github.com/sdcio/data-server/pkg/tree"
	"github.com/sdcio/data-server/pkg/utils"
	"github.com/sdcio/data-server/pkg/utils/testhelper"
	sdcio_schema "github.com/sdcio/data-server/tests/sdcioygot"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

var (
	// TypedValue Bool True and false
	TypedValueTrue  []byte
	TypedValueFalse []byte
)

func init() {
	// Calculate the TypedValues for true and false
	TypedValueTrue, _ = proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_BoolVal{BoolVal: true}})
	TypedValueFalse, _ = proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_BoolVal{BoolVal: true}})
}

func TestDatastore_populateTree(t *testing.T) {
	prio15 := int32(15)
	prio10 := int32(10)
	prio5 := int32(5)
	owner1 := "owner1"
	owner2 := "owner2"
	owner3 := "owner3"

	_ = prio15
	_ = prio10
	_ = prio5
	_ = owner1
	_ = owner2
	_ = owner3

	tests := []struct {
		name                 string
		intentReqValue       func() (string, error) // depending on the path, this should be *testhelper.TestConfig or any sub-value
		intentReqPath        string
		intentName           string
		intentPrio           int32
		intentDelete         bool
		expectedModify       []*cache.Update
		expectedDeletes      [][]string
		expectedOwnerDeletes [][]string
		expectedOwnerUpdates []*cache.Update
		intendedStoreUpdates []*cache.Update
		runningStoreUpdates  []*cache.Update
		NotOnlyNewOrUpdated  bool // it negated when used in the call, usually we want it to be true
	}{
		{
			name:          "DoubleKey - Delete Single item",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Doublekey: map[sdcio_schema.SdcioModel_Doublekey_Key]*sdcio_schema.SdcioModel_Doublekey{
						{
							Key1: "k1.1",
							Key2: "k1.2",
						}: {
							Key1:    ygot.String("k1.1"),
							Key2:    ygot.String("k1.2"),
							Mandato: ygot.String("TheMandatoryValueOther"),
							Cont: &sdcio_schema.SdcioModel_Doublekey_Cont{
								Value1: ygot.String("containerval1.1"),
								Value2: ygot.String("containerval1.2"),
							},
						},
						{
							Key1: "k2.1",
							Key2: "k2.2",
						}: {
							Key1:    ygot.String("k2.1"),
							Key2:    ygot.String("k2.2"),
							Mandato: ygot.String("TheMandatoryValue2"),
							Cont: &sdcio_schema.SdcioModel_Doublekey_Cont{
								Value1: ygot.String("containerval2.1"),
								Value2: ygot.String("containerval2.2"),
							},
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			runningStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key2"}, testhelper.GetStringTvProto(t, "k1.2"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), tree.RunningValuesPrio, tree.RunningIntentName, 0),

				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key2"}, testhelper.GetStringTvProto(t, "k1.3"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), tree.RunningValuesPrio, tree.RunningIntentName, 0),

				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key1"}, testhelper.GetStringTvProto(t, "k2.1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key2"}, testhelper.GetStringTvProto(t, "k2.2"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue2"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval2.1"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval2.2"), tree.RunningValuesPrio, tree.RunningIntentName, 0),
			},
			expectedDeletes: [][]string{
				{"doublekey", "k1.1", "k1.3"},
			},
			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValueOther"), prio10, owner2, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValueOther"), prio10, owner2, 0),
			},
			expectedOwnerDeletes: [][]string{
				{"doublekey", "k1.1", "k1.3", "key1"},
				{"doublekey", "k1.1", "k1.3", "key2"},
				{"doublekey", "k1.1", "k1.3", "mandato"},
				{"doublekey", "k1.1", "k1.3", "cont", "value1"},
				{"doublekey", "k1.1", "k1.3", "cont", "value2"},
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key2"}, testhelper.GetStringTvProto(t, "k1.2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), prio10, owner2, 0),

				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key2"}, testhelper.GetStringTvProto(t, "k1.3"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), prio10, owner2, 0),

				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key1"}, testhelper.GetStringTvProto(t, "k2.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key2"}, testhelper.GetStringTvProto(t, "k2.2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval2.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval2.2"), prio10, owner2, 0),
			},
		},
		{
			name:          "DoubleKey - New Data",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Doublekey: map[sdcio_schema.SdcioModel_Doublekey_Key]*sdcio_schema.SdcioModel_Doublekey{
						{
							Key1: "k1.1",
							Key2: "k1.2",
						}: {
							Key1:    ygot.String("k1.1"),
							Key2:    ygot.String("k1.2"),
							Mandato: ygot.String("TheMandatoryValue1"),
							Cont: &sdcio_schema.SdcioModel_Doublekey_Cont{
								Value1: ygot.String("containerval1.1"),
								Value2: ygot.String("containerval1.2"),
							},
						},
						{
							Key1: "k2.1",
							Key2: "k2.2",
						}: {
							Key1:    ygot.String("k2.1"),
							Key2:    ygot.String("k2.2"),
							Mandato: ygot.String("TheMandatoryValue2"),
							Cont: &sdcio_schema.SdcioModel_Doublekey_Cont{
								Value1: ygot.String("containerval2.1"),
								Value2: ygot.String("containerval2.2"),
							},
						},
						{
							Key1: "k1.1",
							Key2: "k1.3",
						}: {
							Key1:    ygot.String("k1.1"),
							Key2:    ygot.String("k1.3"),
							Mandato: ygot.String("TheMandatoryValue1"),
							Cont: &sdcio_schema.SdcioModel_Doublekey_Cont{
								Value1: ygot.String("containerval1.1"),
								Value2: ygot.String("containerval1.2"),
							},
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key2"}, testhelper.GetStringTvProto(t, "k1.2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), prio10, owner2, 0),

				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key2"}, testhelper.GetStringTvProto(t, "k1.3"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), prio10, owner2, 0),

				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key1"}, testhelper.GetStringTvProto(t, "k2.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key2"}, testhelper.GetStringTvProto(t, "k2.2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval2.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval2.2"), prio10, owner2, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "key2"}, testhelper.GetStringTvProto(t, "k1.2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), prio10, owner2, 0),

				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key1"}, testhelper.GetStringTvProto(t, "k1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "key2"}, testhelper.GetStringTvProto(t, "k1.3"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval1.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k1.1", "k1.3", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval1.2"), prio10, owner2, 0),

				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key1"}, testhelper.GetStringTvProto(t, "k2.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "key2"}, testhelper.GetStringTvProto(t, "k2.2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "mandato"}, testhelper.GetStringTvProto(t, "TheMandatoryValue2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value1"}, testhelper.GetStringTvProto(t, "containerval2.1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"doublekey", "k2.1", "k2.2", "cont", "value2"}, testhelper.GetStringTvProto(t, "containerval2.2"), prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{},
		},
		{
			name:          "Simple add to root path",
			intentName:    owner1,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{},
				}
				d.Interface["ethernet-1/1"] = &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/1"),
					Description: ygot.String("MyDescription"),
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},

			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio10, owner1, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio10, owner1, 0),
			},
			intendedStoreUpdates: nil,
		},
		{
			name:          "Simple add with a specific path",
			intentName:    owner1,
			intentPrio:    prio10,
			intentReqPath: "interface",

			intentReqValue: func() (string, error) {
				i := &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/1"),
					Description: ygot.String("MyDescription"),
				}
				return ygot.EmitJSON(i, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio10, owner1, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio10, owner1, 0),
			},
			intendedStoreUpdates: nil,
		},
		{
			name:          "Add with existing better prio same intent / owner",
			intentName:    owner1,
			intentPrio:    prio10,
			intentReqPath: "interface",
			intentReqValue: func() (string, error) {
				i := &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/1"),
					Description: ygot.String("MyDescription"),
				}
				return ygot.EmitJSON(i, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			expectedModify: []*cache.Update{
				// Right now, although the value stays the same, but the priority changes, we'll receive an update for these values.
				// This maybe needs to be mitigated, but is not considered harmfull atm.
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio10, owner1, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio10, owner1, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio5, owner1, 0),
			},
		},
		{
			name:          "Delete the highes priority values, making shadowed values become active",
			intentName:    owner1,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				return "{}", nil
			},
			intentDelete: true,
			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescriptionOwner2"), prio10, owner2, 0),
			},
			expectedOwnerDeletes: [][]string{
				{"interface", "ethernet-1/1", "name"},
				{"interface", "ethernet-1/1", "description"},
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescriptionOwner2"), prio10, owner2, 0),
			},
		},
		{
			name:          "Delete - aggregate branch via keys",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				return "{}", nil
			},
			intentDelete: true,
			expectedDeletes: [][]string{
				{"interface", "ethernet-1/1"},
			},
			expectedOwnerDeletes: [][]string{
				{"interface", "ethernet-1/1", "name"},
				{"interface", "ethernet-1/1", "description"},
				{"interface", "ethernet-1/1", "admin-state"},
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescriptionOwner2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "admin-state"}, testhelper.GetStringTvProto(t, "enable"), prio10, owner2, 0),
			},
		},
		{
			name:          "Delete - aggregate branch via keys, multiple entries (different keys)",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{
						"ethernet-1/1": {
							Name:        ygot.String("ethernet-1/1"),
							Description: ygot.String("MyNonappliedDescription"),
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			intentDelete: true,
			expectedDeletes: [][]string{
				{"interface", "ethernet-1/1", "admin-state"},
				{"interface", "ethernet-1/2"},
				{"interface", "ethernet-1/3"},
			},
			expectedOwnerDeletes: [][]string{
				{"interface", "ethernet-1/1", "admin-state"},
				{"interface", "ethernet-1/2", "name"},
				{"interface", "ethernet-1/2", "description"},
				{"interface", "ethernet-1/2", "admin-state"},
				{"interface", "ethernet-1/3", "name"},
				{"interface", "ethernet-1/3", "description"},
				{"interface", "ethernet-1/3", "admin-state"},
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyNonappliedDescription"), prio10, owner2, 0),
			},
			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyNonappliedDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescriptionOwner2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "admin-state"}, testhelper.GetStringTvProto(t, "enable"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "MyDescriptionOwner2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "admin-state"}, testhelper.GetStringTvProto(t, "enable"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/3", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/3"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/3", "description"}, testhelper.GetStringTvProto(t, "MyDescriptionOwner2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/3", "admin-state"}, testhelper.GetStringTvProto(t, "enable"), prio10, owner2, 0),
			},
		},
		{
			name:          "Add lower precedence intent, every value already shadowed",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "interface",
			intentReqValue: func() (string, error) {
				i := &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/1"),
					Description: ygot.String("MyNonappliedDescription"),
				}
				return ygot.EmitJSON(i, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},

			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyNonappliedDescription"), prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio5, owner1, 0),
			},
		},
		{
			name:          "Mixed, new entry, higher and lower precedence",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",

			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{},
				}
				d.Interface["ethernet-1/1"] = &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/1"),
					Description: ygot.String("MyOtherDescription"),
					Subinterface: map[uint32]*sdcio_schema.SdcioModel_Interface_Subinterface{
						1: {
							Index:       ygot.Uint32(1),
							Description: ygot.String("Subinterface Desc"),
						},
					},
				}

				d.Interface["ethernet-1/2"] = &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/2"),
					Description: ygot.String("MyOtherDescription"),
					Subinterface: map[uint32]*sdcio_schema.SdcioModel_Interface_Subinterface{
						1: {
							Index:       ygot.Uint32(1),
							Description: ygot.String("Subinterface Desc"),
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},

			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "MyOtherDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyOtherDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "MyOtherDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio15, owner3, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "Owner3 Description"), prio15, owner3, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "2", "index"}, testhelper.GetUIntTvProto(t, 1), prio15, owner3, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "2", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio15, owner3, 0),
			},
		},
		{
			name:                "Mixed, new entry, higher and lower precedence. notOnlyUpdated set to TRUE",
			intentName:          owner2,
			intentPrio:          prio10,
			intentReqPath:       "/",
			NotOnlyNewOrUpdated: true,

			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{},
				}
				d.Interface["ethernet-1/1"] = &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/1"),
					Description: ygot.String("MyOtherDescription"),
					Subinterface: map[uint32]*sdcio_schema.SdcioModel_Interface_Subinterface{
						1: {
							Index:       ygot.Uint32(1),
							Description: ygot.String("Subinterface Desc"),
						},
					},
				}

				d.Interface["ethernet-1/2"] = &sdcio_schema.SdcioModel_Interface{
					Name:        ygot.String("ethernet-1/2"),
					Description: ygot.String("MyOtherDescription"),
					Subinterface: map[uint32]*sdcio_schema.SdcioModel_Interface_Subinterface{
						1: {
							Index:       ygot.Uint32(1),
							Description: ygot.String("Subinterface Desc"),
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},

			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "MyOtherDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
				// the next two are not part of the result, we might want to add the behaviour, such that one can query the entire intended.
				// cache.NewUpdate([]string{"interface", "ethernet-0/1", "subinterface", "2", "index"}, testhelper.GetUIntTvProto(t, 1), prio15, owner3, 0),
				// cache.NewUpdate([]string{"interface", "ethernet-0/1", "subinterface", "2", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio15, owner3, 0),
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyOtherDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "MyOtherDescription"), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "index"}, testhelper.GetUIntTvProto(t, 1), prio10, owner2, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "1", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/1"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/1", "description"}, testhelper.GetStringTvProto(t, "MyDescription"), prio5, owner1, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "name"}, testhelper.GetStringTvProto(t, "ethernet-1/2"), prio15, owner3, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "description"}, testhelper.GetStringTvProto(t, "Owner3 Description"), prio15, owner3, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "2", "index"}, testhelper.GetUIntTvProto(t, 1), prio15, owner3, 0),
				cache.NewUpdate([]string{"interface", "ethernet-1/2", "subinterface", "2", "description"}, testhelper.GetStringTvProto(t, "Subinterface Desc"), prio15, owner3, 0),
			},
		},
		{
			name:          "ChoiceCase - new highes case",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Choices: &sdcio_schema.SdcioModel_Choices{
						Case2: &sdcio_schema.SdcioModel_Choices_Case2{
							Log: ygot.Bool(true),
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			expectedModify: []*cache.Update{
				cache.NewUpdate([]string{"choices", "case2", "log"}, TypedValueTrue, prio10, owner2, 0),
			},
			expectedDeletes: [][]string{
				{"choices", "case1"},
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"choices", "case2", "log"}, TypedValueTrue, prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"choices", "case1", "case-elem", "elem"}, testhelper.GetStringTvProto(t, "case1-content"), prio15, owner1, 0),
				cache.NewUpdate([]string{"choices", "case1", "log"}, TypedValueFalse, prio15, owner1, 0),
			},
		},
		{
			name:          "ChoiceCase - old highes case",
			intentName:    owner2,
			intentPrio:    prio10,
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Choices: &sdcio_schema.SdcioModel_Choices{
						Case2: &sdcio_schema.SdcioModel_Choices_Case2{
							Log: ygot.Bool(true),
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			expectedModify: []*cache.Update{
				// no mods expected
			},
			expectedOwnerUpdates: []*cache.Update{
				cache.NewUpdate([]string{"choices", "case2", "log"}, TypedValueTrue, prio10, owner2, 0),
			},
			intendedStoreUpdates: []*cache.Update{
				cache.NewUpdate([]string{"choices", "case1", "case-elem", "elem"}, testhelper.GetStringTvProto(t, "case1-content"), prio5, owner1, 0),
				cache.NewUpdate([]string{"choices", "case1", "log"}, TypedValueFalse, prio5, owner1, 0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create a gomock controller
			controller := gomock.NewController(t)

			// create a cache client mock
			cacheClient := mockcacheclient.NewMockClient(controller)
			testhelper.ConfigureCacheClientMock(t, cacheClient, tt.intendedStoreUpdates, tt.runningStoreUpdates, tt.expectedModify, tt.expectedDeletes)

			schemaClient, schema, err := testhelper.InitSDCIOSchema()
			if err != nil {
				t.Fatal(err)
			}

			dsName := "dev1"

			// create a datastore
			d := &Datastore{
				config: &config.DatastoreConfig{
					Name:   dsName,
					Schema: schema,
				},

				sbi:          mocktarget.NewMockTarget(controller),
				cacheClient:  cacheClient,
				schemaClient: schemaClient,
			}
			d.treeCacheSchemaClient = tree.NewTreeSchemaCacheClient(dsName, cacheClient, d.getValidationClient())

			ctx := context.Background()

			// marshall the intentReqValue into a byte slice
			jsonConf, err := tt.intentReqValue()
			if err != nil {
				t.Error(err)
			}

			// parse the path under which the intent value is to be put
			path, err := utils.ParsePath(tt.intentReqPath)
			if err != nil {
				t.Error(err)
			}

			// prepare the SetintentRequest
			reqOne := &sdcpb.SetIntentRequest{
				Name:     dsName,
				Intent:   tt.intentName,
				Priority: tt.intentPrio,
				Update: []*sdcpb.Update{
					{
						Path: path,
						Value: &sdcpb.TypedValue{
							Value: &sdcpb.TypedValue_JsonVal{
								JsonVal: []byte(jsonConf)},
						},
					},
				},
				Delete: tt.intentDelete,
			}

			tc := tree.NewTreeContext(d.treeCacheSchemaClient, tt.intentName)

			root, err := tree.NewTreeRoot(ctx, tc)
			if err != nil {
				t.Error(err)
			}

			updSlice, err := d.expandAndConvertIntent(ctx, reqOne.GetIntent(), reqOne.GetPriority(), reqOne.GetUpdate())
			if err != nil {
				t.Error(err)
			}

			oldIntentContent, err := root.LoadIntendedStoreOwnerData(ctx, reqOne.GetIntent(), false)
			if err != nil {
				t.Error(err)
			}

			loadHighest := oldIntentContent.ToPathSet()
			loadHighest.Join(updSlice.ToPathSet())

			err = d.loadIntendedStoreHighestPrio(ctx, d.treeCacheSchemaClient, root, loadHighest, []string{reqOne.GetIntent()})
			if err != nil {
				t.Error(err)
			}

			flags := tree.NewUpdateInsertFlags()
			flags.SetNewFlag()
			root.AddCacheUpdatesRecursive(ctx, updSlice, flags)

			// populate Tree with running
			err = d.populateTreeWithRunning(ctx, d.treeCacheSchemaClient, root)
			if err != nil {
				t.Error(err)
			}

			root.FinishInsertionPhase(ctx)

			validationResult := root.Validate(ctx, false)

			fmt.Printf("Validation Errors:\n%v\n", strings.Join(validationResult.ErrorsStr(), "\n"))
			fmt.Printf("Tree:%s\n", root.String())

			// get the updates that are meant to be send down towards the device
			updates := root.GetHighestPrecedence(!tt.NotOnlyNewOrUpdated)
			if diff := testhelper.DiffCacheUpdates(tt.expectedModify, updates.ToCacheUpdateSlice()); diff != "" {
				t.Errorf("root.GetHighestPrecedence(true) mismatch (-want +got):\n%s", diff)
			}

			// get the deletes that are meant to be send down towards the device
			deletes, err := root.GetDeletes(true)
			if err != nil {
				t.Error(err)
			}

			deletePathSlice := make(tree.PathSlices, 0, len(deletes))
			for _, del := range deletes {
				deletePathSlice = append(deletePathSlice, del.Path())
			}

			if diff := testhelper.DiffDoubleStringPathSlice(tt.expectedDeletes, deletePathSlice.ToStringSlice()); diff != "" {
				t.Errorf("root.GetDeletes() mismatch (-want +got):\n%s", diff)
			}

			// get the updates that are meant to be send down towards the cache (INTENDED)
			updatesOwner := root.GetUpdatesForOwner(tt.intentName)
			if diff := testhelper.DiffCacheUpdates(tt.expectedOwnerUpdates, updatesOwner); diff != "" {
				t.Errorf("root.GetUpdatesForOwner mismatch (-want +got):\n%s", diff)
			}

			// get the deletes that are meant to be send down towards the cache (INTENDED)
			deletesOwner := root.GetDeletesForOwner(tt.intentName)
			if diff := testhelper.DiffDoubleStringPathSlice(tt.expectedOwnerDeletes, deletesOwner.ToStringSlice()); diff != "" {
				t.Errorf("root.GetDeletesForOwner mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
