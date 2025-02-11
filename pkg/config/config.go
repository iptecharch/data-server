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

package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	schemaConfig "github.com/sdcio/schema-server/pkg/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
)

const ()

type Config struct {
	GRPCServer                *GRPCServer                     `yaml:"grpc-server,omitempty" json:"grpc-server,omitempty"`
	SchemaStore               *schemaConfig.SchemaStoreConfig `yaml:"schema-store,omitempty" json:"schema-store,omitempty"`
	Datastores                []*DatastoreConfig              `yaml:"datastores,omitempty" json:"datastores,omitempty"`
	SchemaServer              *RemoteSchemaServer             `yaml:"schema-server,omitempty" json:"schema-server,omitempty"`
	Cache                     *CacheConfig                    `yaml:"cache,omitempty" json:"cache,omitempty"`
	Prometheus                *PromConfig                     `yaml:"prometheus,omitempty" json:"prometheus,omitempty"`
	DefaultTransactionTimeout time.Duration                   `yaml:"transaction-timeout,omitempty" json:"transaction-timeout,omitempty"`
}

type TLS struct {
	CA         string `yaml:"ca,omitempty" json:"ca,omitempty"`
	Cert       string `yaml:"cert,omitempty" json:"cert,omitempty"`
	Key        string `yaml:"key,omitempty" json:"key,omitempty"`
	SkipVerify bool   `yaml:"skip-verify,omitempty" json:"skip-verify,omitempty"`
}

func New(file string) (*Config, error) {
	c := new(Config)
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}

		err = yaml.Unmarshal(b, c)
		if err != nil {
			return nil, err
		}
	}
	err := c.validateSetDefaults()
	return c, err
}

func (c *Config) validateSetDefaults() error {
	if c.GRPCServer == nil {
		c.GRPCServer = &GRPCServer{}
	}
	err := c.GRPCServer.validateSetDefaults()
	if err != nil {
		return err
	}

	// make sure either local or remote schema stores are enabled
	if c.SchemaStore != nil && c.SchemaServer != nil {
		return errors.New("cannot define local schema-store and a remote schema server at the same time")
	}
	// set local schema server config
	if c.SchemaStore == nil && c.SchemaServer == nil {
		c.SchemaStore = &schemaConfig.SchemaStoreConfig{
			Type:    schemaConfig.StoreTypePersistent,
			Schemas: make([]*schemaConfig.SchemaConfig, 0),
		}
		if c.GRPCServer.SchemaServer == nil {
			c.GRPCServer.SchemaServer = &SchemaServer{
				Enabled:          true,
				SchemasDirectory: "/schemas",
			}
		}
	}
	if c.SchemaStore != nil {
		switch c.SchemaStore.Type {
		case "":
			c.SchemaStore.Type = schemaConfig.StoreTypeMemory
		case schemaConfig.StoreTypeMemory:
		case schemaConfig.StoreTypePersistent:
			if c.SchemaStore.Path == "" {
				c.SchemaStore.Path = defaultSchemaStorePath
			}
		default:
			return fmt.Errorf("unknown schema store type %q", c.SchemaStore.Type)
		}
	}
	if c.SchemaStore == nil && (c.GRPCServer.SchemaServer == nil || !c.GRPCServer.SchemaServer.Enabled) {
		return errors.New("schema-server RPCs cannot be exposed if the schema server is not enabled")
	}
	//
	if c.SchemaServer != nil {
		if err = c.SchemaServer.validateSetDefaults(); err != nil {
			return err
		}
	}
	for _, ds := range c.Datastores {
		if err = ds.ValidateSetDefaults(); err != nil {
			return err
		}
	}
	if c.Cache == nil {
		c.Cache = &CacheConfig{}
	}
	if err = c.Cache.validateSetDefaults(); err != nil {
		return err
	}

	if c.DefaultTransactionTimeout == 0 {
		c.DefaultTransactionTimeout = 5 * time.Minute
	}
	return nil
}

type RemoteSchemaServer struct {
	Address string             `yaml:"address,omitempty" json:"address,omitempty"`
	TLS     *TLS               `yaml:"tls,omitempty" json:"tls,omitempty"`
	Cache   *RemoteSchemaCache `yaml:"cache,omitempty" json:"cache,omitempty"`
}

type RemoteSchemaCache struct {
	TTL             time.Duration `yaml:"ttl,omitempty" json:"ttl,omitempty"`
	Capacity        uint64        `yaml:"capacity,omitempty" json:"capacity,omitempty"`
	WithDescription bool          `yaml:"with-description,omitempty" json:"with-description,omitempty"`
	RefreshOnHit    bool          `yaml:"refresh-on-hit,omitempty" json:"refresh-on-hit,omitempty"`
}

func (r *RemoteSchemaServer) validateSetDefaults() error {
	if r.Address == "" {
		return fmt.Errorf("missing remote schema server address")
	}
	if r.Cache != nil {
		if r.Cache.TTL <= 0 {
			r.Cache.TTL = defaultRemoteSchemaServerCacheTTL
		}
		if r.Cache.Capacity == 0 {
			r.Cache.Capacity = defaultRemoteSchemaServerCacheCapacity
		}
	}
	return nil
}

type GRPCServer struct {
	Address        string        `yaml:"address,omitempty" json:"address,omitempty"`
	TLS            *TLS          `yaml:"tls,omitempty" json:"tls,omitempty"`
	SchemaServer   *SchemaServer `yaml:"schema-server,omitempty" json:"schema-server,omitempty"`
	DataServer     *DataServer   `yaml:"data-server,omitempty" json:"data-server,omitempty"`
	MaxRecvMsgSize int           `yaml:"max-recv-msg-size,omitempty" json:"max-recv-msg-size,omitempty"`
	RPCTimeout     time.Duration `yaml:"rpc-timeout,omitempty" json:"rpc-timeout,omitempty"`
}

func (g *GRPCServer) validateSetDefaults() error {
	if g.Address == "" {
		g.Address = defaultGRPCAddress
	}
	if g.MaxRecvMsgSize <= 0 {
		g.MaxRecvMsgSize = defaultMaxRecvMsgSize
	}
	if g.RPCTimeout <= 0 {
		g.RPCTimeout = defaultRPCTimeout
	}
	return nil
}

func (t *TLS) NewConfig(ctx context.Context) (*tls.Config, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: t.SkipVerify}
	if t.CA != "" {
		ca, err := os.ReadFile(t.CA)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA cert: %w", err)
		}
		if len(ca) != 0 {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(ca)
			tlsCfg.RootCAs = caCertPool
		}
	}

	if t.Cert != "" && t.Key != "" {
		certWatcher, err := certwatcher.New(t.Cert, t.Key)
		if err != nil {
			return nil, err
		}

		go func() {
			if err := certWatcher.Start(ctx); err != nil {
				log.Errorf("certificate watcher error: %v", err)
			}
		}()
		tlsCfg.GetCertificate = certWatcher.GetCertificate
	}
	return tlsCfg, nil
}

type DataServer struct {
	// Enabled       bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	MaxCandidates int `yaml:"max-candidates,omitempty" json:"max-candidates,omitempty"`
}

type SchemaServer struct {
	Enabled          bool   `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	SchemasDirectory string `yaml:"schemas-directory,omitempty" json:"schemas-directory,omitempty"`
}
