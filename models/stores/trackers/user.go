package trackers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// UserInactivityThreshold is the maximum gap between LastUpdated and LastDate
// after which a user is presumed inactive and excluded from on-the-fly
// refreshes.
const UserInactivityThreshold = 14 * 24 * time.Hour

type User struct {
	ID            bson.ObjectID            `bson:"_id,omitempty"`
	Username      string                   `bson:"username"`
	UsernameLower string                   `bson:"usernameLower"`
	Level         int                      `bson:"level"`
	AvatarUrl     string                   `bson:"avatarUrl"`
	LastDate      time.Time                `bson:"lastDate"`
	LastUpdated   time.Time                `bson:"lastUpdated,omitempty"`
	LastSeen      time.Time                `bson:"lastSeen,omitempty"`
	OnlineTime    time.Time                `bson:"onlineTime"`
	Wealth        map[string]float64       `bson:"wealth"`
	CaseOpenings  map[string]UserCaseStats `bson:"caseStats"`
	CountryID     bson.ObjectID            `bson:"countryId"`
	CompanyID     *bson.ObjectID           `bson:"companyId,omitempty"`
	PartyID       *bson.ObjectID           `bson:"partyId,omitempty"`
	MuID          *bson.ObjectID           `bson:"muId,omitempty"`
	MilitaryRank  int                      `bson:"militaryRank"`
	Skills        map[string]int           `bson:"skills,omitempty"`
	LatestObject  json.RawMessage          `bson:"raw"`
	RawHash       string                   `bson:"rawHash,omitempty"`
}

type UserCaseStats struct {
	Uncommon  int `bson:"uncommon"`
	Common    int `bson:"common"`
	Rare      int `bson:"rare"`
	Epic      int `bson:"epic"`
	Legendary int `bson:"legendary"`
	Mythic    int `bson:"mythic"`
}

type UserStore struct {
	coll *mongo.Collection
}

func NewUserStore(ctx context.Context, db *mongo.Database) *UserStore {
	store := &UserStore{
		coll: db.Collection("users"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "usernameLower", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on users.usernameLower",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "lastUpdated", Value: 1},
			{Key: "lastDate", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on users.{lastUpdated,lastDate}",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "lastSeen", Value: 1}},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on users.lastSeen",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "companyId", Value: 1}},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on users.companyId",
			"error", err,
		)
	}
}

func (s *UserStore) Exists(ctx context.Context, ids []bson.ObjectID) ([]bson.ObjectID, error) {
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

func (s *UserStore) CreateEmpty(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.InsertOne(ctx, User{ID: id})
	return err
}

func (s *UserStore) GetEmpty(ctx context.Context) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "usernameLower", Value: ""}},
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

func (s *UserStore) Get(ctx context.Context, id bson.ObjectID) (*User, error) {
	var user User
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *UserStore) GetForRefresh(ctx context.Context, n int, exclude []bson.ObjectID, recentThreshold time.Duration) ([]bson.ObjectID, error) {
	if n <= 0 {
		return nil, nil
	}

	thresholdMillis := int64(UserInactivityThreshold / time.Millisecond)
	recentCutoff := time.Now().UTC().Add(-recentThreshold)

	matchUsername := bson.D{
		{Key: "usernameLower", Value: bson.D{{Key: "$ne", Value: ""}}},
	}
	if len(exclude) > 0 {
		matchUsername = append(matchUsername, bson.E{
			Key:   "_id",
			Value: bson.D{{Key: "$nin", Value: exclude}},
		})
	}

	// Huge, right?
	// It:
	// - Skips empty users (still to be filled)
	// - Skips users we're already refreshing (exclude)
	// - Skips "inactive" users, UNLESS we've seen them recently
	// - Top-priority bucket = either lastUpdated < lastDate (existing
	//   heuristic) OR recently seen but not yet refreshed since
	// - Within a bucket, oldest lastUpdated first
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchUsername}},
		{{Key: "$match", Value: bson.D{
			{Key: "$nor", Value: bson.A{
				bson.D{
					{Key: "lastUpdated", Value: bson.D{{Key: "$gt", Value: time.Time{}}}},
					{Key: "lastDate", Value: bson.D{{Key: "$gt", Value: time.Time{}}}},
					{Key: "$expr", Value: bson.D{{Key: "$gt", Value: bson.A{
						bson.D{{Key: "$subtract", Value: bson.A{"$lastUpdated", "$lastDate"}}},
						thresholdMillis,
					}}}},
					{Key: "$or", Value: bson.A{
						bson.D{{Key: "lastSeen", Value: bson.D{{Key: "$exists", Value: false}}}},
						bson.D{{Key: "lastSeen", Value: bson.D{{Key: "$lt", Value: recentCutoff}}}},
					}},
				},
			}},
		}}},
		{{Key: "$addFields", Value: bson.D{
			{Key: "priorityFlag", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$or", Value: bson.A{
					bson.D{{Key: "$lt", Value: bson.A{"$lastUpdated", "$lastDate"}}},
					bson.D{{Key: "$and", Value: bson.A{
						bson.D{{Key: "$gte", Value: bson.A{"$lastSeen", recentCutoff}}},
						bson.D{{Key: "$lt", Value: bson.A{"$lastUpdated", "$lastSeen"}}},
					}}},
				}}},
				0,
				1,
			}}}},
		}}},
		{{Key: "$sort", Value: bson.D{
			{Key: "priorityFlag", Value: 1},
			{Key: "lastUpdated", Value: 1},
		}}},
		{{Key: "$limit", Value: int64(n)}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := s.coll.Aggregate(ctx, pipeline)
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

func (s *UserStore) UpsertUser(ctx context.Context, id bson.ObjectID, data User) error {
	data.ID = id
	hash := hashRaw(data.LatestObject)
	data.RawHash = hash

	var existing struct {
		RawHash string `bson:"rawHash"`
	}
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		options.FindOne().SetProjection(bson.D{{Key: "rawHash", Value: 1}}),
	).Decode(&existing)
	switch {
	case err == nil:
		if hash != "" && existing.RawHash == hash {
			_, err = s.coll.UpdateOne(ctx,
				bson.D{{Key: "_id", Value: id}},
				bson.D{{Key: "$set", Value: bson.D{{Key: "lastUpdated", Value: data.LastUpdated}}}},
			)
			return err
		}
	case errors.Is(err, mongo.ErrNoDocuments):
	default:
		return err
	}

	_, err = s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}

// DistinctCompanyIDs returns the set of non-nil companyIds across the given
// user IDs. Used by the companies scheduler to discover which companies need
// a refresh after a window of wage activity.
func (s *UserStore) DistinctCompanyIDs(ctx context.Context, userIDs []bson.ObjectID) ([]bson.ObjectID, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	filter := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: userIDs}}},
		{Key: "companyId", Value: bson.D{{Key: "$ne", Value: nil}}},
	}

	res := s.coll.Distinct(ctx, "companyId", filter)
	err := res.Err()
	if err != nil {
		return nil, err
	}

	var ids []bson.ObjectID
	err = res.Decode(&ids)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// MarkLastSeen sets lastSeen=now on every existing user in ids. Missing IDs
// are silently skipped — they'll be created by the userqueue and marked on
// the next flush.
func (s *UserStore) MarkLastSeen(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "lastSeen", Value: time.Now().UTC()}}}},
	)
	return err
}

// MembersAllInactive reports whether every given user is both known (already
// populated) and inactive. It returns false if any id is still an empty
// placeholder, so callers never disband an entity on incomplete information. A
// user counts as active when the gap between lastUpdated and lastDate stays
// within UserInactivityThreshold (mirrors the GetForRefresh heuristic).
func (s *UserStore) MembersAllInactive(ctx context.Context, ids []bson.ObjectID) (bool, error) {
	unique := make([]bson.ObjectID, 0, len(ids))
	seen := make(map[bson.ObjectID]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return false, nil
	}

	thresholdMillis := int64(UserInactivityThreshold / time.Millisecond)

	known, err := s.coll.CountDocuments(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: unique}}},
		{Key: "usernameLower", Value: bson.D{{Key: "$ne", Value: ""}}},
	})
	if err != nil {
		return false, err
	}
	if known < int64(len(unique)) {
		// At least one member has not been populated yet; stay conservative.
		return false, nil
	}

	active, err := s.coll.CountDocuments(ctx, bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: unique}}},
		{Key: "usernameLower", Value: bson.D{{Key: "$ne", Value: ""}}},
		{Key: "$expr", Value: bson.D{{Key: "$lte", Value: bson.A{
			bson.D{{Key: "$subtract", Value: bson.A{"$lastUpdated", "$lastDate"}}},
			thresholdMillis,
		}}}},
	})
	if err != nil {
		return false, err
	}

	return active == 0, nil
}
