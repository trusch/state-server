FROM golang:1.13 as builder
WORKDIR /app
COPY --from=augustoroman/v8-lib:6.7.77 /v8/include /usr/include
COPY --from=augustoroman/v8-lib:6.7.77 /v8/lib /usr/lib
COPY . ./
RUN go install -v ./...

FROM gcr.io/distroless/cc
COPY --from=builder /go/bin/v8-server /bin/v8-server
CMD ["/bin/v8-server"]
