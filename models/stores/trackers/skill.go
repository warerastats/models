package trackers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Skill struct {
	ID     bson.ObjectID `bson:"_id,omitempty"`
	UserID bson.ObjectID `bson:"userId"`
	Skills UserSkills    `bson:"skills"`
	Since  time.Time     `bson:"since"`
}

type SkillStore struct {
	coll *mongo.Collection
}

func NewSkillStore(ctx context.Context, db *mongo.Database) *SkillStore {
	store := &SkillStore{
		coll: db.Collection("skills"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *SkillStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on skills.{userId,_id}",
			"error", err,
		)
		return
	}
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

func (s *SkillStore) Create(ctx context.Context, userID bson.ObjectID, skills UserSkills, since time.Time) (bson.ObjectID, error) {
	snap := Skill{
		UserID: userID,
		Skills: skills,
		Since:  since,
	}
	res, err := s.coll.InsertOne(ctx, snap)
	if err != nil {
		return bson.ObjectID{}, err
	}
	id, ok := res.InsertedID.(bson.ObjectID)
	if !ok {
		return bson.ObjectID{}, errors.New("skills insert returned non-ObjectID id")
	}
	return id, nil
}

func (s *SkillStore) GetLatestForUser(ctx context.Context, userID bson.ObjectID) (*Skill, error) {
	var snap Skill
	err := s.coll.FindOne(
		ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&snap)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	return &snap, nil
}
