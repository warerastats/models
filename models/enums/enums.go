package enums

type ItemStatus string

const (
	PERFECT    ItemStatus = "PERFECT"
	ONSALE     ItemStatus = "ONSALE"
	USED       ItemStatus = "USED"
	BROKEN     ItemStatus = "BROKEN"
	DISMANTLED ItemStatus = "DISMANTLED"
)

type Side string

const (
	ATTACKER Side = "ATTACKER"
	DEFENDER Side = "DEFENDER"
)

type TradeSide string

const (
	BUY  TradeSide = "BUY"
	SELL TradeSide = "SELL"
)
