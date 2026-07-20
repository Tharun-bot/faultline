package controlplane

import (
	"time"

	pb "github.com/Tharun-bot/faultline/controlplane/proto"
	"github.com/Tharun-bot/faultline/core"
)

// toCoreRule converts a wire-format proto Rule into our internal
// core.Rule. This function (and its inverse, fromCoreRule) is the ONE
// seam between "how rules look on the wire" and "how rules work
// internally".
func toCoreRule(r *pb.Rule) core.Rule {
	var expiresAt time.Time
	if r.ExpiresAtUnix > 0 {
		expiresAt = time.Unix(r.ExpiresAtUnix, 0)
	}

	return core.Rule{
		ID: r.Id,
		Target: core.Target{
			Service: r.Target.GetService(),
			Method:  r.Target.GetMethod(),
			Client:  r.Target.GetClient(),
		},
		FaultType: core.FaultType(r.FaultType),
		Params: core.Params{
			LatencyMS:      int(r.Params.GetLatencyMs()),
			ErrorCode:      r.Params.GetErrorCode(),
			CorruptPct:     int(r.Params.GetCorruptPct()),
			PartialOKCount: int(r.Params.GetPartialOkCount()),
		},
		Probability: r.Probability,
		Active:      r.Active,
		Version:     int(r.Version),
		ExpiresAt:   expiresAt,
	}
}

// fromCoreRule is the inverse: internal core.Rule -> wire-format proto.
func fromCoreRule(r core.Rule) *pb.Rule {
	var expiresUnix int64
	if !r.ExpiresAt.IsZero() {
		expiresUnix = r.ExpiresAt.Unix()
	}

	return &pb.Rule{
		Id: r.ID,
		Target: &pb.Target{
			Service: r.Target.Service,
			Method:  r.Target.Method,
			Client:  r.Target.Client,
		},
		FaultType: string(r.FaultType),
		Params: &pb.Params{
			LatencyMs:      int32(r.Params.LatencyMS),
			ErrorCode:      r.Params.ErrorCode,
			CorruptPct:     int32(r.Params.CorruptPct),
			PartialOkCount: int32(r.Params.PartialOKCount),
		},
		Probability:   r.Probability,
		Active:        r.Active,
		Version:       int32(r.Version),
		ExpiresAtUnix: expiresUnix,
	}
}
