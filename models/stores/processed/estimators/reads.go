package estimators

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// flipPageLimit clamps a requested page size into a sane range.
func flipPageLimit(limit int) int64 {
	switch {
	case limit > 200:
		return 200
	case limit > 0:
		return int64(limit)
	default:
		return 20
	}
}

// GetByUserRange returns a user's flip events whose at falls in [from, to],
// newest first, keyset-paginated on _id (the sell trade id).
func (s *UserFlipEventStore) GetByUserRange(ctx context.Context, userID bson.ObjectID, from, to time.Time, before *bson.ObjectID, limit int) ([]UserFlipEvent, error) {
	filter := bson.D{
		{Key: "userId", Value: userID},
		{Key: "at", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lte", Value: to}}},
	}
	if before != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	cursor, err := s.coll.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(flipPageLimit(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []UserFlipEvent
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByCountryRange returns a country's flip events whose at falls in [from, to],
// newest first, keyset-paginated on _id (the sell trade id).
func (s *CountryFlipEventStore) GetByCountryRange(ctx context.Context, countryID bson.ObjectID, from, to time.Time, before *bson.ObjectID, limit int) ([]CountryFlipEvent, error) {
	filter := bson.D{
		{Key: "countryId", Value: countryID},
		{Key: "at", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lte", Value: to}}},
	}
	if before != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	cursor, err := s.coll.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(flipPageLimit(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CountryFlipEvent
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TopByProfit returns the users with the highest cumulative flip profit.
func (s *UserFlipStateStore) TopByProfit(ctx context.Context, limit int) ([]UserFlipState, error) {
	cursor, err := s.coll.Find(ctx, bson.D{},
		options.Find().
			SetSort(bson.D{{Key: "totalProfit", Value: -1}}).
			SetLimit(flipPageLimit(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []UserFlipState
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetRange returns daily inflation points whose dayStart falls in [from, to],
// oldest first.
func (s *InflationStore) GetRange(ctx context.Context, from, to time.Time) ([]InflationPoint, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "dayStart", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lte", Value: to}}}},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []InflationPoint
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
