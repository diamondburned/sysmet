package sysmet

import (
	"bytes"
	"io/fs"
	"os"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// LegacyDatabase is a database instance for V1 or V2.
type LegacyDatabase struct {
	db *bbolt.DB
}

// OpenLegacy opens a legacy database. If exclusive is true, then the
// legacy database is opened with O_EXCL, meaning we cannot override any
// existing database.
func OpenLegacy(path string) (*LegacyDatabase, error) {
	return openLegacy(path, false)
}

// OpenLegacyExcl opens a legacy database with exclusive mode, so the legacy
// database is opened with O_EXCL, meaning we cannot override any existing
// database.
func OpenLegacyExisting(path string) (*LegacyDatabase, error) {
	return openLegacy(path, true)
}

func openLegacy(path string, existingOnly bool) (*LegacyDatabase, error) {
	db, err := bbolt.Open(path, os.ModePerm, &bbolt.Options{
		Timeout:      5 * time.Second,
		FreelistType: bbolt.FreelistArrayType,
		OpenFile: func(path string, flags int, mode fs.FileMode) (*os.File, error) {
			if existingOnly {
				flags &= ^os.O_CREATE
				flags |= os.O_EXCL
			}
			return os.OpenFile(path, flags, mode)
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "bbolt")
	}

	return &LegacyDatabase{db}, nil
}

func (db *LegacyDatabase) Close() error {
	return db.db.Close()
}

// MigrateV1toV2 migrates the database from v1 to v2.
func (db *LegacyDatabase) MigrateV1toV2(max time.Duration) error {
	if db.db.IsReadOnly() {
		return errors.New("database not writable")
	}

	now := convertWithUnixZero(time.Now())
	sec := uint32(max / time.Second)

	before := unixToBE(now - sec)

	return db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNameV1)
		if b == nil {
			return nil // nothing to do.
		}

		cs := b.Cursor()
		var s Snapshot
		var buf bytes.Buffer
		buf.Grow(1024)

		for k, v := cs.Seek(before); k != nil; k, v = cs.Next() {
			// Try and delete. Ignore if it's a bucket.
			if err := cs.Delete(); err != nil {
				continue
			}

			s = Snapshot{}
			buf.Reset()

			if err := decodeSnapshot(v, &s); err != nil {
				continue
			}

			// Re-encode.
			if err := encodeSnapshotBuf(s, &buf); err != nil {
				// If we can't reencode a valid snapshot, then that's not good.
				return errors.Wrap(err, "cannot encode snapshot")
			}

			if err := b.Put(k, buf.Bytes()); err != nil {
				return errors.Wrap(err, "cannot put into database")
			}
		}

		return nil
	})
}

// allow doing 250 values per transaction. Hopefully, this will make badger free
// up some memory after each transaction.
const migrateV3BatchSize = 250

// MigrateV2toV3 migrates the database from v2 to v3. The user should make a
// copy of the v2 database before opening a v3 database and running this.
func (db *LegacyDatabase) MigrateV2toV3(v3db *Database) error {
	if v3db.ro {
		return errors.New("v3 database not writable")
	}

	bboltTx, err := db.db.Begin(false)
	if err != nil {
		return errors.Wrap(err, "cannot begin bbolt transaction")
	}
	defer bboltTx.Rollback()

	b := bboltTx.Bucket(bucketNameV2)
	if b == nil {
		return nil // nothing to do.
	}

	cs := b.Cursor()
	begin := unixToBE(0)

	var txCount int
	var badgerTx *badger.Txn

	for k, v := cs.Seek(begin); k != nil; k, v = cs.Next() {
		if v == nil {
			continue // is bucket
		}

		if badgerTx == nil || txCount >= migrateV3BatchSize {
			if badgerTx != nil {
				if err := badgerTx.Commit(); err != nil {
					return errors.Wrap(err, "cannot commit badger transaction")
				}

				// Trigger a GC to potentially free up some memory.
				runtime.GC()
			}

			badgerTx = v3db.db.NewTransaction(true)
			// I'm aware that this will run at the end of the function, not the
			// loop. This is intentional, since we want to run Discard even if
			// Commit didn't fail.
			defer badgerTx.Discard()
		}

		// Copy the value since badger will be holding it.
		// This is honestly super wasteful and we're far better off just doing
		// multiple transactions, but that's a lot of work. We hope we won't run
		// out of memory doing this.
		value := append([]byte(nil), v...)

		if err := badgerTx.Set(bkey(bPoints, k), value); err != nil {
			return errors.Wrap(err, "cannot set in badger")
		}
	}

	if badgerTx != nil {
		if err := badgerTx.Commit(); err != nil {
			return errors.Wrap(err, "cannot commit badger transaction")
		}
	}

	return nil
}
