package statsiglocal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	statsigEpoch = int64(1682924400)
	statsigSalt  = "obfiowerehiring"
	statsigMark  = byte(0x03)

	// This matched seed/fingerprint pair is public protocol material, not an
	// account credential. A fresh timestamp and mask are generated per request.
	seedBase64  = "t2ODAFY4ozXd0K2Y8MdI2XfxTDiJoakZPuoaKfcQn8VuasZMcKliyhA1pJ+o1oMf"
	fingerprint = "3bab9506b851eb851eb840e8f5c28f5c28f80e8f5c28f5c28f806b851eb851eb8400"
)

var protocolSeed = mustDecodeSeed(seedBase64)

// Generate creates the 70-byte x-statsig-id accepted by Grok Web for one
// method/path pair. nowUnix must be the current Unix timestamp.
func Generate(pathname, method string, nowUnix int64) (string, error) {
	var mask [1]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return "", fmt.Errorf("generate Statsig mask: %w", err)
	}
	return generateWithMask(pathname, method, nowUnix, mask[0])
}

func generateWithMask(pathname, method string, nowUnix int64, mask byte) (string, error) {
	pathname = strings.TrimSpace(pathname)
	if pathname == "" || pathname[0] != '/' {
		return "", errors.New("Statsig path must be absolute")
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return "", errors.New("Statsig method is required")
	}
	delta := nowUnix - statsigEpoch
	if delta < 0 || delta > math.MaxUint32 {
		return "", errors.New("Statsig timestamp is outside the supported range")
	}
	number := uint32(delta)

	input := method + "!" + pathname + "!" + strconv.FormatUint(uint64(number), 10) + statsigSalt + fingerprint
	digest := sha256.Sum256([]byte(input))
	tail := make([]byte, 21)
	binary.LittleEndian.PutUint32(tail[:4], number)
	copy(tail[4:20], digest[:16])
	tail[20] = statsigMark

	output := make([]byte, 70)
	output[0] = mask
	for index, value := range protocolSeed {
		output[index+1] = value ^ mask
	}
	for index, value := range tail {
		output[index+49] = value ^ mask
	}
	return base64.RawStdEncoding.EncodeToString(output), nil
}

func mustDecodeSeed(value string) []byte {
	seed, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(seed) != 48 {
		panic("invalid embedded Statsig seed")
	}
	return seed
}
