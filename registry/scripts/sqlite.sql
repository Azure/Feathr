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
create table userroles
(
    record_id     int auto_increment
        primary key,
    project_name  varchar(255) not null,
    user_name     varchar(255) not null,
    role_name     varchar(50)  not null,
    create_by     varchar(255) not null,
    create_reason text         not null,
    create_time   datetime     not null,
    delete_by     varchar(255) null,
    delete_reason text         null,
    delete_time   datetime     null
);

create index create_by
    on userroles (create_by);

create index delete_by
    on userroles (delete_by);

create index project_name
    on userroles (project_name);

create index role_name
    on userroles (role_name);

create index user_name
    on userroles (user_name);

