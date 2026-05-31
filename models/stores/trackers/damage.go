package trackers

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Damage struct {
	ID           bson.ObjectID `bson:"_id,omitempty"`
	BattleID     bson.ObjectID `bson:"battleId"`
	UserID       bson.ObjectID `bson:"userId"`
	WeaponID     bson.ObjectID `bson:"weaponId"`
	HelmetID     bson.ObjectID `bson:"helmetId"`
	ChestID      bson.ObjectID `bson:"chestId"`
	PantsID      bson.ObjectID `bson:"pantsId"`
	BootsID      bson.ObjectID `bson:"bootsId"`
	GlovesID     bson.ObjectID `bson:"glovesId"`
	SkillID      bson.ObjectID `bson:"skillId"`
	MilitaryRank int           `bson:"militaryRank"`
}

type DamageStore struct {
	coll *mongo.Collection
}

func NewDamageStore(ctx context.Context, db *mongo.Database) *DamageStore {
	store := &DamageStore{
		coll: db.Collection("damages"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *DamageStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "battleId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on damages.battleId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on damages.userId",
			"error", err,
		)
		return
	}
}
