CREATE TABLE entities(
    entity_id varchar(50),
    entity_content text,
    PRIMARY KEY (entity_id)
);
CREATE TABLE edges(
    from_id varchar(50),
    to_id varchar(50),
    edge_type varchar(50),
    PRIMARY KEY (from_id, to_id, edge_type)
);
