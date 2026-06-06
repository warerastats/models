package trackers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Damage struct {
	ID           bson.ObjectID  `bson:"_id,omitempty"`
	BattleID     bson.ObjectID  `bson:"battleId"`
	Side         enums.Side     `bson:"side"`
	UserID       bson.ObjectID  `bson:"userId"`
	CountryID    bson.ObjectID  `bson:"countryId"`
	MuID         *bson.ObjectID `bson:"muId,omitempty"`
	PartyID      *bson.ObjectID `bson:"partyId,omitempty"`
	WeaponID     *bson.ObjectID `bson:"weaponId,omitempty"`
	Ammo         *string        `bson:"ammo,omitempty"`
	HelmetID     *bson.ObjectID `bson:"helmetId,omitempty"`
	ChestID      *bson.ObjectID `bson:"chestId,omitempty"`
	PantsID      *bson.ObjectID `bson:"pantsId,omitempty"`
	BootsID      *bson.ObjectID `bson:"bootsId,omitempty"`
	GlovesID     *bson.ObjectID `bson:"glovesId,omitempty"`
	SkillID      bson.ObjectID  `bson:"skillId"`
	MilitaryRank int            `bson:"militaryRank"`
	Damages      int            `bson:"damages"`
	At           time.Time      `bson:"at"`
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
			{Key: "userId", Value: 1},
			{Key: "side", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on damages.{battleId,userId,side}",
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

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "partyId", Value: 1},
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on damages.{partyId,_id}",
			"error", err,
		)
		return
	}
}

func (s *DamageStore) Create(ctx context.Context, d Damage) (bson.ObjectID, error) {
	res, err := s.coll.InsertOne(ctx, d)
	if err != nil {
		return bson.ObjectID{}, err
	}
	id, ok := res.InsertedID.(bson.ObjectID)
	if !ok {
		return bson.ObjectID{}, errors.New("damages insert returned non-ObjectID id")
	}
	return id, nil
}

// BulkCreate inserts a batch of damage events in a single unordered write.
// Damage attribution is delta-based and self-healing, so an unordered insert
// (where one failing document does not abort the rest) is safe: any dropped
// delta is re-derived on the next ranking sweep.
func (s *DamageStore) BulkCreate(ctx context.Context, ds []Damage) error {
	if len(ds) == 0 {
		return nil
	}
	docs := make([]any, len(ds))
	for i := range ds {
		docs[i] = ds[i]
	}
	_, err := s.coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	return err
}

func (s *DamageStore) GetUserBattleTotal(ctx context.Context, battleID, userID bson.ObjectID, side enums.Side) (int, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "battleId", Value: battleID},
			{Key: "userId", Value: userID},
			{Key: "side", Value: side},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$damages"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		err = cursor.Err()

		if err != nil {
			return 0, err
		}

		return 0, nil
	}
	var result struct {
		Total int `bson:"total"`
	}

	err = cursor.Decode(&result)
	if err != nil {
		return 0, err
	}

	return result.Total, nil
}

// ByUserAndBattle returns every damage record for the given user in the given
// battle. Backed by the {battleId,userId,side} compound index.
func (s *DamageStore) ByUserAndBattle(ctx context.Context, userID, battleID bson.ObjectID) ([]Damage, error) {
	cursor, err := s.coll.Find(ctx, bson.D{
		{Key: "battleId", Value: battleID},
		{Key: "userId", Value: userID},
	}, options.Find().SetSort(bson.D{{Key: "at", Value: -1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Damage
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
