-- Create table
create table JOB51
(
  id           VARCHAR2(32) not null,
  code         VARCHAR2(12) not null,
  name         VARCHAR2(200),
  addr         VARCHAR2(32),
  salary       VARCHAR2(32),
  company      VARCHAR2(100),
  company_info VARCHAR2(150),
  year         VARCHAR2(32),
  education    VARCHAR2(32),
  num          VARCHAR2(20),
  release      VARCHAR2(10),
  language     VARCHAR2(32),
  type         VARCHAR2(100),
  welfare      VARCHAR2(150),
  jbdetail     VARCHAR2(4000),
  addr_detail  VARCHAR2(200)
) tablespace USERS;
-- Create/Recreate primary, unique and foreign key constraints
alter table JOB51
  add primary key (ID)
  using index
  tablespace USERS;
