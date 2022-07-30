create table entities
(
    entity_id      varchar(50) not null
        primary key,
    entity_content text  not null
);

create table edges
(
    from_id   varchar(50) not null,
    to_id     varchar(50) not null,
    edge_type varchar(20) not null
);

create index entity_dep_conn_type_index
    on edges (edge_type);

create index entity_dep_from_id_index
    on edges (from_id);

create index entity_dep_to_id_index
    on edges (to_id);

create table userroles
(
    record_id     SERIAL
        primary key,
    project_name  varchar(255) not null,
    user_name     varchar(255) not null,
    role_name     varchar(50)  not null,
    create_by     varchar(255) not null,
    create_reason text         not null,
    create_time   TIMESTAMPTZ  not null,
    delete_by     varchar(255) null,
    delete_reason text         null,
    delete_time   TIMESTAMPTZ  null
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

