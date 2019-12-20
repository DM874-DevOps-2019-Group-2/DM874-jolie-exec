CREATE TYPE script_type as ENUM ('send', 'recv');

CREATE TABLE user_scripts_enabled (
    user_id integer NOT NULL,
    direction script_type NOT NULL,
    PRIMARY KEY(user_id, direction),
    CONSTRAINT no_duplicate_enties UNIQUE (user_id, direction)
);
