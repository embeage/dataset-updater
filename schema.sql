CREATE TABLE Videos (
    id CHAR(7) PRIMARY KEY,
    name TEXT NOT NULL,
    duration INTEGER NOT NULL,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    sweden_only BOOLEAN NOT NULL,
    url TEXT,
    short_description TEXT,
    long_description TEXT,
    production_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE VideoEncodings (
    id SERIAL PRIMARY KEY,
    video CHAR(7) REFERENCES Videos ON DELETE CASCADE,
    bandwidth INTEGER NOT NULL,
    codecs TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    segment_length FLOAT NOT NULL,
    segment_sizes INTEGER[] NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE AudioEncodings (
    id SERIAL PRIMARY KEY,
    video CHAR(7) UNIQUE REFERENCES Videos ON DELETE CASCADE,
    bandwidth INTEGER NOT NULL,
    codecs TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    sampling_rate INTEGER NOT NULL,
    segment_length FLOAT NOT NULL,
    segment_sizes INTEGER[] NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Genres (
    id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE VideoGenres (
    video CHAR(7) REFERENCES Videos ON DELETE CASCADE,
    genre TEXT REFERENCES Genres ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (video, genre)
);
