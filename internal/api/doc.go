// Package api exposes the REST and WebSocket surface compatible with the
// original NestJS service. Trading workers consume the same persistent models
// and event stream, so HTTP requests never block chain subscription handling.
package api
