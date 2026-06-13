package trackers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strings"
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
	ID               bson.ObjectID            `bson:"_id,omitempty"`
	Username         string                   `bson:"username"`
	UsernameLower    string                   `bson:"usernameLower"`
	Level            int                      `bson:"level"`
	AvatarUrl        string                   `bson:"avatarUrl"`
	LastDate         time.Time                `bson:"lastDate"`
	LastUpdated      time.Time                `bson:"lastUpdated,omitempty"`
	LastSeen         time.Time                `bson:"lastSeen,omitempty"`
	OnlineTime       time.Time                `bson:"onlineTime"`
	Wealth           map[string]float64       `bson:"wealth"`
	CaseOpenings     map[string]UserCaseStats `bson:"caseStats"`
	CountryID        bson.ObjectID            `bson:"countryId"`
	CompanyID        *bson.ObjectID           `bson:"companyId,omitempty"`
	PartyID          *bson.ObjectID           `bson:"partyId,omitempty"`
	MuID             *bson.ObjectID           `bson:"muId,omitempty"`
	MilitaryRank     int                      `bson:"militaryRank"`
	Skills           map[string]int           `bson:"skills,omitempty"`
	LastCompanyCheck time.Time                `bson:"lastCompanyCheck,omitempty"`
	LatestObject     json.RawMessage          `bson:"raw"`
	RawHash          string                   `bson:"rawHash,omitempty"`
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
		Keys: bson.D{
			{Key: "_id", Value: 1},
			{Key: "companyId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on users.{_id,companyId}",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on users.{countryId,_id}",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "lastCompanyCheck", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on users.lastCompanyCheck", "error", err)
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

	// baseFilter selects populated, non-excluded, "active" users: the $nor
	// clause drops users that are inactive (refresh gap beyond the threshold)
	// unless they've been seen recently. extraExclude is merged into the
	// in-flight exclude set so the second pass never re-returns first-pass hits.
	baseFilter := func(extraExclude []bson.ObjectID) bson.D {
		f := bson.D{
			{Key: "usernameLower", Value: bson.D{{Key: "$ne", Value: ""}}},
		}

		ex := exclude
		if len(extraExclude) > 0 {
			ex = make([]bson.ObjectID, 0, len(exclude)+len(extraExclude))
			ex = append(ex, exclude...)
			ex = append(ex, extraExclude...)
		}
		if len(ex) > 0 {
			f = append(f, bson.E{Key: "_id", Value: bson.D{{Key: "$nin", Value: ex}}})
		}

		f = append(f, bson.E{Key: "$nor", Value: bson.A{
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
		}})
		return f
	}

	// priorityOr is the high-priority bucket: either the user has activity newer
	// than its last refresh (lastUpdated < lastDate), or it was seen recently but
	// hasn't been refreshed since (lastUpdated < lastSeen). Equivalent to the
	// previous priorityFlag == 0 condition.
	priorityOr := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "$lt", Value: bson.A{"$lastUpdated", "$lastDate"}}},
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "$gte", Value: bson.A{"$lastSeen", recentCutoff}}},
			bson.D{{Key: "$lt", Value: bson.A{"$lastUpdated", "$lastSeen"}}},
		}}},
	}}}

	findIDs := func(filter bson.D, limit int) ([]bson.ObjectID, error) {
		if limit <= 0 {
			return nil, nil
		}
		cursor, err := s.coll.Find(ctx, filter,
			options.Find().
				SetSort(bson.D{{Key: "lastUpdated", Value: 1}}).
				SetLimit(int64(limit)).
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
		return ids, cursor.Err()
	}

	// Pass 1: high-priority bucket, oldest lastUpdated first.
	priorityFilter := append(baseFilter(nil), bson.E{Key: "$expr", Value: priorityOr})
	priority, err := findIDs(priorityFilter, n)
	if err != nil {
		return nil, err
	}
	if len(priority) >= n {
		return priority, nil
	}

	// Pass 2: everything else, oldest lastUpdated first, excluding pass-1 hits.
	// Appending bucket 1 after bucket 0 reproduces the old {priorityFlag,
	// lastUpdated} ordering exactly.
	fillerFilter := append(baseFilter(priority), bson.E{
		Key:   "$expr",
		Value: bson.D{{Key: "$not", Value: bson.A{priorityOr}}},
	})
	filler, err := findIDs(fillerFilter, n-len(priority))
	if err != nil {
		return nil, err
	}

	return append(priority, filler...), nil
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

// Search returns up to limit users whose username starts with term
// (case-insensitive), ordered alphabetically. Prefix-anchored so the
// usernameLower index is used.
func (s *UserStore) Search(ctx context.Context, term string, limit int) ([]User, error) {
	if term == "" || limit <= 0 {
		return nil, nil
	}
	pattern := "^" + regexp.QuoteMeta(strings.ToLower(term))
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "usernameLower", Value: bson.D{{Key: "$regex", Value: pattern}}}},
		options.Find().
			SetSort(bson.D{{Key: "usernameLower", Value: 1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []User
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetLastCompanyCheck stamps the given user's lastCompanyCheck to t.
func (s *UserStore) SetLastCompanyCheck(ctx context.Context, id bson.ObjectID, t time.Time) error {
	_, err := s.coll.UpdateByID(ctx, id, bson.D{{Key: "$set", Value: bson.D{{Key: "lastCompanyCheck", Value: t}}}})
	return err
}

// GetForCompanyOwnershipCheck returns up to n populated, active user IDs ordered by oldest lastCompanyCheck.
func (s *UserStore) GetForCompanyOwnershipCheck(ctx context.Context, n int, exclude []bson.ObjectID) ([]bson.ObjectID, error) {
	if n <= 0 {
		return nil, nil
	}

	thresholdMillis := int64(UserInactivityThreshold / time.Millisecond)

	filter := bson.D{
		{Key: "usernameLower", Value: bson.D{{Key: "$ne", Value: ""}}},
	}
	if len(exclude) > 0 {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$nin", Value: exclude}}})
	}
	// Exclude inactive users (same heuristic as GetForRefresh).
	filter = append(filter, bson.E{Key: "$nor", Value: bson.A{
		bson.D{
			{Key: "lastUpdated", Value: bson.D{{Key: "$gt", Value: time.Time{}}}},
			{Key: "lastDate", Value: bson.D{{Key: "$gt", Value: time.Time{}}}},
			{Key: "$expr", Value: bson.D{{Key: "$gt", Value: bson.A{
				bson.D{{Key: "$subtract", Value: bson.A{"$lastUpdated", "$lastDate"}}},
				thresholdMillis,
			}}}},
		},
	}})

	cursor, err := s.coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "lastCompanyCheck", Value: 1}}).
			SetProjection(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(int64(n)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var ids []bson.ObjectID
	for cursor.Next(ctx) {
		var row struct {
			ID bson.ObjectID `bson:"_id"`
		}
		if err := cursor.Decode(&row); err != nil {
			return nil, err
		}
		ids = append(ids, row.ID)
	}
	return ids, cursor.Err()
}
