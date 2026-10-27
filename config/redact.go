package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
)

// Redacted returns a deep copy of c in which every non-empty JWT key secret and every API key hash
// is replaced by "REDACTED". c itself is not modified.
func (c *Config) Redacted() *Config {
	r := deepCopy(reflect.ValueOf(c)).Interface().(*Config)
	for i := range r.Auth.JWT {
		for k := range r.Auth.JWT[i].Keys {
			if r.Auth.JWT[i].Keys[k].Secret != "" {
				r.Auth.JWT[i].Keys[k].Secret = "REDACTED"
			}
		}
	}
	for i := range r.Auth.APIKeys {
		for k := range r.Auth.APIKeys[i].Keys {
			if r.Auth.APIKeys[i].Keys[k].Hash != "" {
				r.Auth.APIKeys[i].Keys[k].Hash = "REDACTED"
			}
		}
	}
	return r
}

// Fingerprint returns the lowercase hex SHA-256 of the JSON encoding of c. encoding/json sorts map
// keys, so the encoding is canonical. For configurations built in Go whose parameters cannot be
// encoded as JSON, the fingerprint is the SHA-256 of "unencodable: " followed by the encoding error.
func (c *Config) Fingerprint() string {
	b, err := json.Marshal(c)
	if err != nil {
		sum := sha256.Sum256([]byte("unencodable: " + err.Error()))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func deepCopy(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.New(v.Type().Elem())
		n.Elem().Set(deepCopy(v.Elem()))
		return n
	case reflect.Struct:
		n := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			n.Field(i).Set(deepCopy(v.Field(i)))
		}
		return n
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			n.Index(i).Set(deepCopy(v.Index(i)))
		}
		return n
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			n.SetMapIndex(iter.Key(), deepCopy(iter.Value()))
		}
		return n
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.New(v.Type()).Elem()
		n.Set(deepCopy(v.Elem()))
		return n
	default:
		return v
	}
}
