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

package clients

import (
	schema_server "github.com/sdcio/sdc-protos/sdcpb"

	"github.com/sdcio/data-server/pkg/cache"
	CacheClient "github.com/sdcio/data-server/pkg/datastore/clients/cache"
	SchemaClient "github.com/sdcio/data-server/pkg/datastore/clients/schema"
	"github.com/sdcio/data-server/pkg/schema"
)

type ValidationClientImpl struct {
	CacheClient.CacheClientBound
	SchemaClient.SchemaClientBound
}

func NewValidationClient(datastoreName string, c cache.Client, s *schema_server.Schema, sc schema.Client) *ValidationClientImpl {
	return &ValidationClientImpl{
		CacheClientBound:  CacheClient.NewCacheClientBound(datastoreName, c),
		SchemaClientBound: SchemaClient.NewSchemaClientBound(s, sc),
	}
}

// ValidationClient provides a client that bundles the bound clients for the cache as well as for the schema, of a certain device.
type ValidationClient interface {
	CacheClient.CacheClientBound
	SchemaClient.SchemaClientBound
}
