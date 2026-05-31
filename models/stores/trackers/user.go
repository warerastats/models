package trackers

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type User struct {
	ID            bson.ObjectID      `bson:"id,omitempty"`
	Username      string             `bson:"username"`
	UsernameLower string             `bson:"usernameLower"`
	Level         int                `bson:"level"`
	AvatarUrl     string             `bson:"avatarUrl"`
	LastDate      time.Time          `bson:"lastDate"`
	OnlineTime    time.Time          `bson:"onlineTime"`
	Skills        UserSkills         `bson:"skills"`
	Wealth        map[string]float64 `bson:"wealth"`
	CaseOpenings  UserCaseStats      `bson:"caseStats"`
	CountryID     bson.ObjectID      `bson:"countryId"`
	CompanyID     bson.ObjectID      `bson:"companyId"`
	PartyID       bson.ObjectID      `bson:"partyId"`
	MuID          bson.ObjectID      `bson:"muId"`
}

type UserSkills struct {
	Energy           int `bson:"energy"`
	Health           int `bson:"health"`
	Hunger           int `bson:"hunger"`
	Attack           int `bson:"attack"`
	Companies        int `bson:"companies"`
	Entrepreneurship int `bson:"entrepreneurship"`
	Production       int `bson:"production"`
	CriticalChance   int `bson:"criticalChance"`
	CriticalDamages  int `bson:"criticalDamages"`
	Armor            int `bson:"armor"`
	Precision        int `bson:"precision"`
	Dodge            int `bson:"dodge"`
	LootChance       int `bson:"lootChance"`
	Management       int `bson:"management"`
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
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on users.id",
			"error", err,
		)
		return
	}
}
