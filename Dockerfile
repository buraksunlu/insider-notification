# One image per binary: docker build --build-arg CMD=api|worker|mockprovider .
# The migrate target bundles the migrations: docker build --target migrate .
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=api
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# Migrations are copied into the image instead of bind-mounted, so the
# stack also starts where the checkout is not visible to the Docker daemon
# (Colima or a remote daemon with the repo outside the shared folders).
FROM migrate/migrate:v4.18.1 AS migrate
COPY migrations /migrations

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
