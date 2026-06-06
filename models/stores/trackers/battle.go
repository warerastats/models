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

type Battle struct {
	ID                bson.ObjectID   `bson:"_id"`
	AttackerRegionID  *bson.ObjectID  `bson:"attackerRegionId,omitempty"`
	AttackerCountryID bson.ObjectID   `bson:"attackerCountryId"`
	AttackerDamages   int             `bson:"attackerDamages"`
	DefenderRegionID  bson.ObjectID   `bson:"defenderRegionId"`
	DefenderCountryID bson.ObjectID   `bson:"defenderCountryId"`
	DefenderDamages   int             `bson:"defenderDamages"`
	WinnerSide        *string         `bson:"winnerSide,omitempty"`
	IsActive          bool            `bson:"active"`
	EndedAt           *time.Time      `bson:"endedAt,omitempty"`
	LastUpdated       time.Time       `bson:"updated"`
	LatestObject      json.RawMessage `bson:"raw"`
}

type BattleStore struct {
	coll *mongo.Collection
}

func NewBattleStore(ctx context.Context, db *mongo.Database) *BattleStore {
	store := &BattleStore{
		coll: db.Collection("battles"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *BattleStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on battles._id",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "active", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on battles.active",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "winnerSide", Value: 1},
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on battles.{winnerSide,_id}",
			"error", err,
		)
	}
}

func (s *BattleStore) Get(ctx context.Context, id bson.ObjectID) (*Battle, error) {
	var battle Battle
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&battle)
	if err != nil {
		return nil, err
	}
	return &battle, nil
}

func (s *BattleStore) UpsertBattle(ctx context.Context, id bson.ObjectID, data Battle) error {
	data.ID = id
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (s *BattleStore) GetActiveIDs(ctx context.Context) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "active", Value: true}},
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

		err := cursor.Decode(&result)
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

// GetUnfinalized returns battles that were marked inactive by the region
// reconciliation path but never finalised (winnerSide still nil). These are
// typically battles that ended while the BattleRanking in-memory tracking map
// was empty, so BattleFinalize never ran on them, leaving damages / endedAt / winnerSide unset.
func (s *BattleStore) GetUnfinalized(ctx context.Context) ([]bson.ObjectID, error) {
	filter := bson.D{
		{Key: "active", Value: false},
		{Key: "winnerSide", Value: nil},
	}
	cursor, err := s.coll.Find(ctx, filter,
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

func (s *BattleStore) MarkInactive(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "active", Value: false},
			{Key: "updated", Value: time.Now().UTC()},
		}}},
	)
	return err
}

func (s *BattleStore) GetInScope(ctx context.Context, since time.Time) ([]bson.ObjectID, error) {
	cutoff := bson.NewObjectIDFromTimestamp(since)
	filter := bson.D{
		{Key: "winnerSide", Value: nil},
		{Key: "_id", Value: bson.D{{Key: "$gte", Value: cutoff}}},
	}
	cursor, err := s.coll.Find(ctx, filter,
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

func (s *BattleStore) SetWinner(
	ctx context.Context,
	id bson.ObjectID,
	winnerSide string,
	endedAt time.Time,
	attackerDamages, defenderDamages int,
	raw json.RawMessage,
) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "winnerSide", Value: winnerSide},
			{Key: "endedAt", Value: endedAt},
			{Key: "attackerDamages", Value: attackerDamages},
			{Key: "defenderDamages", Value: defenderDamages},
			{Key: "active", Value: false},
			{Key: "updated", Value: time.Now().UTC()},
			{Key: "raw", Value: raw},
		}}},
	)
	return err
}
