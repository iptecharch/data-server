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
	"slices"
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
)

func TestDatastore_validateTree(t *testing.T) {
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
		intendedStoreUpdates []*cache.Update
		NotOnlyNewOrUpdated  bool // it negated when used in the call, usually we want it to be true
		expectedWarnings     []string
	}{

		{
			name:          "deref",
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{
						"ethernet-1/1": {
							Name:          ygot.String("ethernet-1/1"),
							InterfaceType: ygot.String("traffic"),
							AdminState:    sdcio_schema.SdcioModelIf_AdminState_enable,
						},
						"mgmt0": {
							Name:          ygot.String("mgmt0"),
							InterfaceType: ygot.String("mgmt"),
							AdminState:    sdcio_schema.SdcioModelIf_AdminState_enable,
						},
					},
					MgmtInterface: &sdcio_schema.SdcioModel_MgmtInterface{
						Name: ygot.String("mgmt0"),
						Type: ygot.String("mgmt"),
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			intentName: owner1,
			intentPrio: prio10,
		},
		{
			name:          "identityref",
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{
						"ethernet-1/1": {
							Name:          ygot.String("ethernet-1/1"),
							InterfaceType: ygot.String("traffic"),
							AdminState:    sdcio_schema.SdcioModelIf_AdminState_enable,
						},
						"mgmt0": {
							Name:          ygot.String("mgmt0"),
							InterfaceType: ygot.String("mgmt"),
							AdminState:    sdcio_schema.SdcioModelIf_AdminState_enable,
						},
					},
					MgmtInterface: &sdcio_schema.SdcioModel_MgmtInterface{
						Name: ygot.String("mgmt0"),
						Type: ygot.String("mgmt"),
					},
					Identityref: &sdcio_schema.SdcioModel_Identityref{
						CryptoA: sdcio_schema.SdcioModelIdentityBase_CryptoAlg_des3,
						CryptoB: sdcio_schema.SdcioModelIdentityBase_CryptoAlg_otherAlgo,
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			intentName: owner1,
			intentPrio: prio10,
		},
		{
			name:          "leafref-optional (require-instance == false) not exists",
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				return "{\"leafref-optional\":\"mgmt0\"}", nil
			},
			intentName:       owner1,
			intentPrio:       prio10,
			expectedWarnings: []string{"leafref leafref-optional value mgmt0 unable to resolve non-mandatory reference /interface/name"},
		},
		{
			name:          "leafref-optional (require-instance == false) exists",
			intentReqPath: "/",
			intentReqValue: func() (string, error) {
				d := &sdcio_schema.Device{
					LeafrefOptional: ygot.String("mgmt0"),
					Interface: map[string]*sdcio_schema.SdcioModel_Interface{
						"mgmt0": {
							Name:        ygot.String("mgmt0"),
							Description: ygot.String("foo"),
						},
					},
				}
				return ygot.EmitJSON(d, &ygot.EmitJSONConfig{
					Format:         ygot.RFC7951,
					SkipValidation: false,
				})
			},
			intentName: owner1,
			intentPrio: prio10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create a gomock controller
			controller := gomock.NewController(t)

			// create a cache client mock
			cacheClient := mockcacheclient.NewMockClient(controller)
			testhelper.ConfigureCacheClientMock(t, cacheClient, tt.intendedStoreUpdates, nil, nil, nil)

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

			tc := tree.NewTreeContext(tree.NewTreeSchemaCacheClient(dsName, d.cacheClient, d.getValidationClient()), tt.intentName)
			root, err := tree.NewTreeRoot(ctx, tc)
			if err != nil {
				t.Error(err)
			}

			insertUpdates := []*sdcpb.Update{
				{
					Path: path,
					Value: &sdcpb.TypedValue{
						Value: &sdcpb.TypedValue_JsonVal{
							JsonVal: []byte(jsonConf)},
					},
				},
			}

			updSlice, err := d.expandAndConvertIntent(ctx, tt.intentName, tt.intentPrio, insertUpdates)
			if err != nil {
				t.Error(err)
			}

			flagsNew := tree.NewUpdateInsertFlags()
			flagsNew.SetNewFlag()
			root.AddCacheUpdatesRecursive(ctx, updSlice, flagsNew)

			root.FinishInsertionPhase(ctx)

			validationResult := root.Validate(ctx, false)

			for _, x := range tt.expectedWarnings {
				if !slices.Contains(validationResult.WarningsStr(), x) {
					t.Errorf("Warning %q not emitted.", x)
				}
			}

			fmt.Println(validationResult.ErrorsStr())
			fmt.Println(validationResult.WarningsStr())

			fmt.Printf("Tree:%s\n", root.String())
		})
	}
}
