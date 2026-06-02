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

type User struct {
	ID            bson.ObjectID            `bson:"_id,omitempty"`
	Username      string                   `bson:"username"`
	UsernameLower string                   `bson:"usernameLower"`
	Level         int                      `bson:"level"`
	AvatarUrl     string                   `bson:"avatarUrl"`
	LastDate      time.Time                `bson:"lastDate"`
	OnlineTime    time.Time                `bson:"onlineTime"`
	Wealth        map[string]float64       `bson:"wealth"`
	CaseOpenings  map[string]UserCaseStats `bson:"caseStats"`
	CountryID     bson.ObjectID            `bson:"countryId"`
	CompanyID     bson.ObjectID            `bson:"companyId"`
	PartyID       bson.ObjectID            `bson:"partyId"`
	MuID          bson.ObjectID            `bson:"muId"`
	MilitaryRank  int                      `bson:"militaryRank"`
	LatestObject  json.RawMessage          `bson:"raw"`
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
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on users._id",
			"error", err,
		)
		return
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
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		existing[result.ID] = struct{}{}
	}
	if err := cursor.Err(); err != nil {
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
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		ids = append(ids, result.ID)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *UserStore) UpsertUser(ctx context.Context, id bson.ObjectID, data User) error {
	data.ID = id
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}
