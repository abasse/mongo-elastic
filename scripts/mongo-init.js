// Initialise the replica set so that MongoDB change streams are available.
// This script runs once when the mongo container starts for the first time.
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "mongo:27017" }],
});
