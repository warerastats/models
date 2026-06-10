package trackers

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Party struct {
	ID            bson.ObjectID   `bson:"_id"`
	Name          string          `bson:"name"`
	NameLower     string          `bson:"nameLower"`
	Description   string          `bson:"description"`
	CountryID     bson.ObjectID   `bson:"countryId"`
	RegionID      bson.ObjectID   `bson:"regionId"`
	LeaderUserID  bson.ObjectID   `bson:"leaderId"`
	MemberUserIDs []bson.ObjectID `bson:"members"`
	AvatarUrl     string          `bson:"avatarUrl"`
	Ethics        struct {
		Unethical     bool `bson:"unethical"`
		Militarism    int  `bson:"militarism"`
		Isolationism  int  `bson:"isolationism"`
		Imperialism   int  `bson:"imperialism"`
		Industrialism int  `bson:"industrialism"`
	} `bson:"ethics"`
	LastUpdated  time.Time       `bson:"lastUpdated,omitempty"`
	LastSeen     time.Time       `bson:"lastSeen,omitempty"`
	DisbandedAt  time.Time       `bson:"disbandedAt,omitempty"`
	LatestObject json.RawMessage `bson:"raw"`
}

type PartyStore struct {
	coll *mongo.Collection
}

func NewPartyStore(ctx context.Context, db *mongo.Database) *PartyStore {
	store := &PartyStore{
		coll: db.Collection("parties"),
	}
	store.ensureIndex(ctx)
	store.migrate(ctx)
	return store
}

func (s *PartyStore) ensureIndex(ctx context.Context) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "countryId", Value: 1}}},
		{Keys: bson.D{{Key: "regionId", Value: 1}}},
		{Keys: bson.D{{Key: "name", Value: 1}}},
		{Keys: bson.D{{Key: "nameLower", Value: 1}}},
		{Keys: bson.D{{Key: "lastUpdated", Value: 1}}},
		{Keys: bson.D{{Key: "lastSeen", Value: 1}}},
		{Keys: bson.D{{Key: "disbandedAt", Value: 1}}},
	}
	_, err := s.coll.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		slog.Error(
			"Failed creating indexes on parties",
			"error", err,
		)
	}
}

// migrate backfills fields for documents that were written before the field
// existed.
func (s *PartyStore) migrate(ctx context.Context) {
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "nameLower", Value: bson.D{{Key: "$exists", Value: false}}}},
		mongo.Pipeline{
			{{Key: "$set", Value: bson.D{
				{Key: "nameLower", Value: bson.D{{Key: "$toLower", Value: "$name"}}},
			}}},
		},
	)
	if err != nil {
		slog.Error("Failed backfilling parties.nameLower", "error", err)
	}
}

// Exists returns the subset of ids that have no tracker document yet.
func (s *PartyStore) Exists(ctx context.Context, ids []bson.ObjectID) ([]bson.ObjectID, error) {
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
// name marks it for backfill by the party scheduler.
func (s *PartyStore) CreateEmpty(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.InsertOne(ctx, Party{ID: id})
	return err
}

// GetEmpty returns the ids of placeholder documents that still need their
// first fetch (name still empty).
func (s *PartyStore) GetEmpty(ctx context.Context) ([]bson.ObjectID, error) {
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

func (s *PartyStore) Get(ctx context.Context, id bson.ObjectID) (*Party, error) {
	var party Party
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&party)
	if err != nil {
		return nil, err
	}
	return &party, nil
}

// GetForRefresh returns up to n populated, non-disbanded parties ordered oldest
// first by lastUpdated, skipping the given exclude set. A disbanded party
// becomes eligible again once its lastSeen is newer than disbandedAt (revival).
func (s *PartyStore) GetForRefresh(ctx context.Context, n int, exclude []bson.ObjectID) ([]bson.ObjectID, error) {
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

// GetStaleAmong returns the subset of the given party ids that are populated
// and whose last refresh is older than olderThan. It ignores the disband flag:
// ruling parties must be refreshed on a fixed cadence regardless of activity.
func (s *PartyStore) GetStaleAmong(ctx context.Context, ids []bson.ObjectID, olderThan time.Duration) ([]bson.ObjectID, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	cutoff := time.Now().UTC().Add(-olderThan)
	filter := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
		{Key: "name", Value: bson.D{{Key: "$ne", Value: ""}}},
		{Key: "lastUpdated", Value: bson.D{{Key: "$lt", Value: cutoff}}},
	}

	cursor, err := s.coll.Find(ctx, filter,
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var stale []bson.ObjectID
	for cursor.Next(ctx) {
		var result struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		stale = append(stale, result.ID)
	}

	err = cursor.Err()
	if err != nil {
		return nil, err
	}

	return stale, nil
}

// UpsertParty writes the latest snapshot, stamping lastUpdated. It never
// touches lastSeen or disbandedAt so the revival and disband lifecycle is
// preserved.
func (s *PartyStore) UpsertParty(ctx context.Context, id bson.ObjectID, data Party) error {
	set := bson.D{
		{Key: "name", Value: data.Name},
		{Key: "nameLower", Value: data.NameLower},
		{Key: "description", Value: data.Description},
		{Key: "countryId", Value: data.CountryID},
		{Key: "regionId", Value: data.RegionID},
		{Key: "leaderId", Value: data.LeaderUserID},
		{Key: "members", Value: data.MemberUserIDs},
		{Key: "avatarUrl", Value: data.AvatarUrl},
		{Key: "ethics", Value: data.Ethics},
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

// MarkLastSeen stamps lastSeen=now on every existing party in ids. A disbanded
// party whose lastSeen overtakes disbandedAt is thereby revived for refresh.
func (s *PartyStore) MarkLastSeen(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "lastSeen", Value: time.Now().UTC()}}}},
	)
	return err
}

// MarkDisbanded flags the party as disbanded as of now, excluding it from
// refresh until it is seen again.
func (s *PartyStore) MarkDisbanded(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "disbandedAt", Value: time.Now().UTC()}}}},
	)
	return err
}

// Search returns up to limit parties whose name starts with term
// (case-insensitive), ordered alphabetically. Prefix-anchored so the nameLower
// index is used.
func (s *PartyStore) Search(ctx context.Context, term string, limit int) ([]Party, error) {
	if term == "" || limit <= 0 {
		return nil, nil
	}
	pattern := "^" + regexp.QuoteMeta(strings.ToLower(term))
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "nameLower", Value: bson.D{{Key: "$regex", Value: pattern}}}},
		options.Find().
			SetSort(bson.D{{Key: "nameLower", Value: 1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Party
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClearDisbanded removes the disbanded flag, returning the party to the normal
// refresh rotation.
func (s *PartyStore) ClearDisbanded(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$unset", Value: bson.D{{Key: "disbandedAt", Value: ""}}}},
	)
	return err
}
