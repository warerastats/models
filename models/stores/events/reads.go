package events

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// MuBattleCount is the number of distinct battles a MU set an order in.
type MuBattleCount struct {
	MuID    bson.ObjectID `bson:"_id"`
	Battles int           `bson:"battles"`
}

// CountByMu returns, per MU, the count of distinct battles it added a MU order in.
func (s *BattleOrderChangeStore) CountByMu(ctx context.Context) ([]MuBattleCount, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "kind", Value: "mu"},
			{Key: "action", Value: "added"},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "muId", Value: "$entityId"},
				{Key: "battleId", Value: "$battleId"},
			}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$_id.muId"},
			{Key: "battles", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MuBattleCount
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CountByMuFinalized returns, per MU, the count of distinct finalized battles it
// added a MU order in (joined to the battles collection by winnerSide).
func (s *BattleOrderChangeStore) CountByMuFinalized(ctx context.Context) ([]MuBattleCount, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "kind", Value: "mu"},
			{Key: "action", Value: "added"},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "muId", Value: "$entityId"},
				{Key: "battleId", Value: "$battleId"},
			}},
		}}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "battles"},
			{Key: "localField", Value: "_id.battleId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "battle"},
		}}},
		{{Key: "$unwind", Value: "$battle"}},
		{{Key: "$match", Value: bson.D{
			{Key: "battle.winnerSide", Value: bson.D{{Key: "$ne", Value: nil}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$_id.muId"},
			{Key: "battles", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MuBattleCount
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BattleMuOrder pairs a battle with a MU that set an order in it.
type BattleMuOrder struct {
	BattleID bson.ObjectID `bson:"battleId"`
	MuID     bson.ObjectID `bson:"muId"`
}

// MuOrdersByBattles returns the distinct (battle, MU) order pairs for the given battles.
func (s *BattleOrderChangeStore) MuOrdersByBattles(ctx context.Context, battleIDs []bson.ObjectID) ([]BattleMuOrder, error) {
	if len(battleIDs) == 0 {
		return nil, nil
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "kind", Value: "mu"},
			{Key: "action", Value: "added"},
			{Key: "battleId", Value: bson.D{{Key: "$in", Value: battleIDs}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "battleId", Value: "$battleId"},
				{Key: "muId", Value: "$entityId"},
			}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "battleId", Value: "$_id.battleId"},
			{Key: "muId", Value: "$_id.muId"},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []BattleMuOrder
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
