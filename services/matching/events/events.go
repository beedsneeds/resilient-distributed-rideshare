package events

import "encoding/json"

// RideAcOutbox payload for a ride.accepted event. DRY.
type RideAcceptedPayload struct {
	DriverID string `json:"driverID"`
}

func (p RideAcceptedPayload) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

func UnmarshalRideAcceptedPayload(payload []byte) (RideAcceptedPayload, error) {
	var p RideAcceptedPayload
	err := json.Unmarshal(payload, &p)
	return p, err
}
