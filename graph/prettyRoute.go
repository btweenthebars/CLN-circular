package graph

import (
	"fmt"
	"strconv"
)

type PrettyRouteHop struct {
	Id             string `json:"id"`
	Alias          string `json:"alias"`
	ShortChannelId string `json:"short_channel_id"`
	MilliSatoshi   uint64 `json:"millisatoshi"`
	Delay          uint   `json:"delay"`
	Fee            uint64 `json:"fee"`
	FeePPM         uint64 `json:"ppm"`
	OutboundFee    uint64 `json:"outbound_fee"`
	InboundFee     int64  `json:"inbound_fee"`
}

type PrettyRoute struct {
	PaymentHash        string           `json:"payment_hash"`
	SourceId           string           `json:"source_id"`
	DestinationId      string           `json:"destination_id"`
	SourceAlias        string           `json:"source_alias"`
	DestinationAlias   string           `json:"destination_alias"`
	Amount             uint64           `json:"amount_sat"`
	Fee                uint64           `json:"fee_msat"`
	FeePPM             uint64           `json:"ppm"`
	Hops               []PrettyRouteHop `json:"hops"`
	InboundSavingsMSat int64            `json:"inbound_savings_msat"`
	InboundSavingsPPM  int64            `json:"inbound_savings_ppm"`
}

func NewPrettyRoute(route *Route, paymentHash string) *PrettyRoute {
	hops := make([]PrettyRouteHop, len(route.Hops))

	// now hops
	from := route.Hops[0].Source
	hops[0] = PrettyRouteHop{
		Id:             from,
		ShortChannelId: route.Hops[0].ShortChannelId,
		MilliSatoshi:   route.Hops[0].MilliSatoshi,
		Delay:          route.Hops[0].Delay,
		Fee:            0,
		FeePPM:         0,
		OutboundFee:    0,
		InboundFee:     0,
	}

	hops[0].Alias = route.Graph.GetAlias(from)

	for i := 1; i < len(route.Hops); i++ {
		fee := route.Hops[i-1].MilliSatoshi - route.Hops[i].MilliSatoshi
		feePPM := fee * 1000000 / route.Hops[i].MilliSatoshi
		from = route.Hops[i].Source
		
		amountToForward := route.Hops[i].MilliSatoshi
		outboundFee := route.Hops[i].ComputeFee(amountToForward)
		inboundFee := route.Graph.GetInboundFee(route.Hops[i-1].Channel, amountToForward)

		hops[i] = PrettyRouteHop{
			Id:             from,
			ShortChannelId: route.Hops[i].ShortChannelId,
			MilliSatoshi:   route.Hops[i].MilliSatoshi,
			Delay:          route.Hops[i].Delay,
			Fee:            fee,
			FeePPM:         feePPM,
			OutboundFee:    outboundFee,
			InboundFee:     inboundFee,
		}
		hops[i].Alias = route.Graph.GetAlias(from)
	}

	feeWithout := route.GetFeeWithoutInboundFee()
	feeWithoutPPM := uint64(0)
	if route.Amount > 0 {
		feeWithoutPPM = (feeWithout * 1000000) / route.Amount
	}
	actualFee := route.Fee()
	actualFeePPM := route.FeePPM()

	return &PrettyRoute{
		PaymentHash:        paymentHash,
		SourceId:           route.Source,
		DestinationId:      route.Destination,
		SourceAlias:        route.Graph.GetAlias(route.Source),
		DestinationAlias:   route.Graph.GetAlias(route.Destination),
		Amount:             route.Amount / 1000,
		Fee:                actualFee,
		FeePPM:             actualFeePPM,
		Hops:               hops,
		InboundSavingsMSat: int64(feeWithout) - int64(actualFee),
		InboundSavingsPPM:  int64(feeWithoutPPM) - int64(actualFeePPM),
	}
}

func (r *PrettyRoute) String() string {
	var result string
	result += "Route from: " + r.SourceAlias + " to: " + r.DestinationAlias + "\n"
	result += "Amount: " + strconv.FormatUint(r.Amount, 10) + "\n"
	result += "Fee: " + strconv.FormatUint(r.Fee, 10) + "msat\n"
	result += "Fee PPM: " + strconv.FormatUint(r.FeePPM, 10) + "\n"
	if r.InboundSavingsMSat != 0 {
		if r.InboundSavingsMSat > 0 {
			result += fmt.Sprintf("Inbound Fee Savings: %d msat (%d PPM)\n", r.InboundSavingsMSat, r.InboundSavingsPPM)
		} else {
			result += fmt.Sprintf("Inbound Fee Overspent: %d msat (%d PPM)\n", -r.InboundSavingsMSat, -r.InboundSavingsPPM)
		}
	}
	result += "Hops: " + strconv.Itoa(len(r.Hops)) + "\n"

	for i := 0; i < len(r.Hops); i++ {
		alias := r.Hops[i].Alias
		fee := r.Hops[i].Fee
		feePPM := r.Hops[i].FeePPM
		delay := r.Hops[i].Delay
		shortChannelId := r.Hops[i].ShortChannelId

		if i > 0 && r.Hops[i].InboundFee != 0 {
			result += fmt.Sprintf("Hop %2d: %40s, fee: %8.3f msat, ppm: %5d, scid: %s, delay: %d (outbound: %.3f msat, inbound: %.3f msat)\n",
				i+1, alias,
				float64(fee)/1000, feePPM,
				shortChannelId, delay,
				float64(r.Hops[i].OutboundFee)/1000,
				float64(r.Hops[i].InboundFee)/1000)
		} else {
			result += fmt.Sprintf("Hop %2d: %40s, fee: %8.3f msat, ppm: %5d, scid: %s, delay: %d\n",
				i+1, alias,
				float64(fee)/1000, feePPM,
				shortChannelId, delay)
		}
	}
	return result
}

func (r *PrettyRoute) Simple() string {
	var result string
	result += "Sending " + strconv.FormatUint(r.Amount, 10) + " sats from [" + r.SourceAlias + "] to [" + r.DestinationAlias
	result += "] over " + strconv.Itoa(len(r.Hops)) + " hops, costing " + strconv.FormatUint(r.Fee, 10) + "msat ("
	result += strconv.FormatUint(r.FeePPM, 10) + "PPM)"
	
	if r.InboundSavingsMSat != 0 {
		if r.InboundSavingsMSat > 0 {
			result += fmt.Sprintf(" [Saved %dmsat (%dPPM) due to inbound discount]", r.InboundSavingsMSat, r.InboundSavingsPPM)
		} else {
			result += fmt.Sprintf(" [Extra %dmsat (%dPPM) due to inbound surcharge]", -r.InboundSavingsMSat, -r.InboundSavingsPPM)
		}
	}
	
	result += " via "
	for i := 0; i < len(r.Hops); i++ {
		alias := r.Hops[i].Alias
		feePPM := r.Hops[i].FeePPM
		result += "- " + alias + " (" + strconv.FormatUint(feePPM, 10) + "PPM) "
	}
	return result
}
