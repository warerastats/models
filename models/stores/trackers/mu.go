package trackers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Mu struct {
	ID                  bson.ObjectID   `bson:"_id"`
	OwnerUserID         bson.ObjectID   `bson:"userId"`
	RegionID            bson.ObjectID   `bson:"regionId"`
	Name                string          `bson:"name"`
	NameLower           string          `bson:"nameLower"`
	AvatarUrl           string          `bson:"avatarUrl"`
	Level               int             `bson:"level"`
	HeadQuarterLevel    int             `bson:"hq"`
	DormitoriesLevel    int             `bson:"dorms"`
	MercenaryReputation float64         `bson:"mercRep"`
	MemberUserIDs       []bson.ObjectID `bson:"members"`
	LastUpdated         time.Time       `bson:"lastUpdated,omitempty"`
	LastSeen            time.Time       `bson:"lastSeen,omitempty"`
	DisbandedAt         time.Time       `bson:"disbandedAt,omitempty"`
	LatestObject        json.RawMessage `bson:"raw"`
}

type MuStore struct {
	coll *mongo.Collection
}

func NewMuStore(ctx context.Context, db *mongo.Database) *MuStore {
	store := &MuStore{
		coll: db.Collection("mus"),
	}
	store.ensureIndex(ctx)
	store.migrate(ctx)
	return store
}

func (s *MuStore) ensureIndex(ctx context.Context) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "name", Value: 1}}},
		{Keys: bson.D{{Key: "nameLower", Value: 1}}},
		{Keys: bson.D{{Key: "lastUpdated", Value: 1}}},
		{Keys: bson.D{{Key: "lastSeen", Value: 1}}},
		{Keys: bson.D{{Key: "disbandedAt", Value: 1}}},
	}
	_, err := s.coll.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		slog.Error(
			"Failed creating indexes on mus",
			"error", err,
		)
	}
}

// migrate backfills fields for documents that were written before the field
// existed.
func (s *MuStore) migrate(ctx context.Context) {
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "nameLower", Value: bson.D{{Key: "$exists", Value: false}}}},
		mongo.Pipeline{
			{{Key: "$set", Value: bson.D{
				{Key: "nameLower", Value: bson.D{{Key: "$toLower", Value: "$name"}}},
			}}},
		},
	)
	if err != nil {
		slog.Error("Failed backfilling mus.nameLower", "error", err)
	}
}

// Exists returns the subset of ids that have no tracker document yet.
func (s *MuStore) Exists(ctx context.Context, ids []bson.ObjectID) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	existing := make(map[bson.ObjectID]struct{}, len(ids))
	for cursor.Next(ctx) {
		var result struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		existing[result.ID] = struct{}{}
	}

	err = cursor.Err()
	if err != nil {
		return nil, err
	}

	var missing []bson.ObjectID
	for _, id := range ids {
		if _, ok := existing[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

// CreateEmpty inserts a placeholder document holding only the id. The empty
// name marks it for backfill by the mu scheduler.
func (s *MuStore) CreateEmpty(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.InsertOne(ctx, Mu{ID: id})
	return err
}

// GetEmpty returns the ids of placeholder documents that still need their
// first fetch (name still empty).
func (s *MuStore) GetEmpty(ctx context.Context) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "name", Value: ""}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var ids []bson.ObjectID
	for cursor.Next(ctx) {
		var result struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		ids = append(ids, result.ID)
	}

	err = cursor.Err()
	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (s *MuStore) Get(ctx context.Context, id bson.ObjectID) (*Mu, error) {
	var mu Mu
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&mu)
	if err != nil {
		return nil, err
	}
	return &mu, nil
}

// GetForRefresh returns up to n populated, non-disbanded mus ordered oldest
// first by lastUpdated, skipping the given exclude set. A disbanded mu becomes
// eligible again once its lastSeen is newer than disbandedAt (revival).
func (s *MuStore) GetForRefresh(ctx context.Context, n int, exclude []bson.ObjectID) ([]bson.ObjectID, error) {
	if n <= 0 {
		return nil, nil
	}

	filter := bson.D{
		{Key: "name", Value: bson.D{{Key: "$ne", Value: ""}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "disbandedAt", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "$expr", Value: bson.D{{Key: "$gt", Value: bson.A{"$lastSeen", "$disbandedAt"}}}}},
		}},
	}
	if len(exclude) > 0 {
		filter = append(filter, bson.E{
			Key:   "_id",
			Value: bson.D{{Key: "$nin", Value: exclude}},
		})
	}

	cursor, err := s.coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "lastUpdated", Value: 1}}).
			SetLimit(int64(n)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var ids []bson.ObjectID
	for cursor.Next(ctx) {
		var result struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		ids = append(ids, result.ID)
	}

	err = cursor.Err()
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// UpsertMu writes the latest snapshot, stamping lastUpdated. It never touches
// lastSeen or disbandedAt so the revival and disband lifecycle is preserved.
func (s *MuStore) UpsertMu(ctx context.Context, id bson.ObjectID, data Mu) error {
	set := bson.D{
		{Key: "userId", Value: data.OwnerUserID},
		{Key: "regionId", Value: data.RegionID},
		{Key: "name", Value: data.Name},
		{Key: "nameLower", Value: data.NameLower},
		{Key: "avatarUrl", Value: data.AvatarUrl},
		{Key: "level", Value: data.Level},
		{Key: "hq", Value: data.HeadQuarterLevel},
		{Key: "dorms", Value: data.DormitoriesLevel},
		{Key: "mercRep", Value: data.MercenaryReputation},
		{Key: "members", Value: data.MemberUserIDs},
		{Key: "raw", Value: data.LatestObject},
		{Key: "lastUpdated", Value: time.Now().UTC()},
	}
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: set}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// MarkLastSeen stamps lastSeen=now on every existing mu in ids. A disbanded mu
// whose lastSeen overtakes disbandedAt is thereby revived for refresh.
func (s *MuStore) MarkLastSeen(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "lastSeen", Value: time.Now().UTC()}}}},
	)
	return err
}

// MarkDisbanded flags the mu as disbanded as of now, excluding it from refresh
// until it is seen again.
func (s *MuStore) MarkDisbanded(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "disbandedAt", Value: time.Now().UTC()}}}},
	)
	return err
}

// ClearDisbanded removes the disbanded flag, returning the mu to the normal
// refresh rotation.
func (s *MuStore) ClearDisbanded(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$unset", Value: bson.D{{Key: "disbandedAt", Value: ""}}}},
	)
	return err
}
