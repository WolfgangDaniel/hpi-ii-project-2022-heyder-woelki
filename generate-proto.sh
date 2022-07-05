#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/person/v1/lobbyPerson.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/person/v1/corporatePerson.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/person/v1/person.proto

# protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto

# protoc --proto_path=proto --python_out=build/gen proto/bakdata/lobby/v1/lobby.proto
