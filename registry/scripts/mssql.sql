create table entities
(
    entity_id      varchar(50)   not null
        primary key,
    entity_content nvarchar(max) not null
)
go

create table edges
(
    from_id   varchar(50) not null,
    to_id     varchar(50) not null,
    edge_type varchar(50) not null,
    constraint edges_pk
        primary key (from_id, to_id, edge_type)
)
go

create table userroles
(
    record_id     int identity,
    project_name  varchar(100) not null,
    user_name     varchar(100) not null,
    role_name     varchar(100) not null,
    create_by     varchar(100) not null,
    create_reason nvarchar(max) not null,
    create_time   datetime    not null,
    delete_by     varchar(100),
    delete_reason nvarchar(max),
    delete_time   datetime
)

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
go
