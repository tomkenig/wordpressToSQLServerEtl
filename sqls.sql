/*
Configure MySQL's
Configuration of ODBC Driver is also needed 
*/

--linuxpl.com
EXEC master.dbo.sp_addlinkedserver 
@server = N'', 
@srvproduct=N'', 
@provider=N'MSDASQL', 
@datasrc=N'', 
@provstr=N'DRIVER=(MySQL ODBC 8.0 Unicode Driver);SERVER=;PORT=3306;DATABASE=; USER= ;PASSWORD=;OPTION=3;', @catalog=N''


EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'DS_s57_linuxpl_com', @useself=N'False', @locallogin=NULL, @rmtuser=NULL, @rmtpassword=NULL
GO

EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'collation compatible', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'data access', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'dist', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'pub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'rpc', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'rpc out', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'sub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'connect timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'collation name', @optvalue=null
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'lazy schema validation', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'query timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'use remote collation', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'DS_s57_linuxpl_com', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO


/*****************
create WP tables 
*****************/


create database wpress
go

use wpress
go

create schema dim
go

--drop table wpress.dim.wp_sites;
create table wpress.dim.wp_sites
(id bigint identity(1,1),
site_name nvarchar(255),
site_url nvarchar(255),
linked_server nvarchar(255),
linked_db  nvarchar(255),
ins_date datetime default getdate(),
ins_date_utc datetime default getutcdate()
);

/*
select * from wpress.dim.wp_sites;
*/


INSERT INTO wpress.dim.wp_sites (site_name, site_url, linked_server, linked_db) VALUES ('','','','');
INSERT INTO wpress.dim.wp_sites (site_name, site_url, linked_server, linked_db) VALUES ('','','','');
INSERT INTO wpress.dim.wp_sites (site_name, site_url, linked_server, linked_db) VALUES ('','','','');
(...)





create schema wp
go


--drop table wp.wp_posts;
CREATE TABLE wp.wp_posts (
  ID bigint,
  post_author bigint,
  post_date datetime,
  post_date_gmt datetime,
  post_content ntext,
  post_title nvarchar(max),
  post_excerpt ntext,
  post_status nvarchar(20),
  comment_status nvarchar(20),
  ping_status nvarchar(20),
  post_password nvarchar(255),
  post_name nvarchar(200),
  to_ping text,
  pinged text,
  post_modified datetime,
  post_modified_gmt datetime,
  post_content_filtered ntext,
  post_parent bigint,
  guid nvarchar(255),
  menu_order bigint,
  post_type nvarchar(20),
  post_mime_type nvarchar(100),
  comment_count bigint,
  --tomkenig: additional columns
  wp_site_id bigint, --site_id (ref to dim.wp_sites.id)
  id_ins bigint identity(1,1), --record id
  ins_date datetime default getdate(),
  ins_date_utc datetime default getutcdate()
) ;


--variable, openquery, linkedserver: https://support.microsoft.com/en-us/help/314520/how-to-pass-a-variable-to-a-linked-server-query


DECLARE @OPENQUERY nvarchar(4000);
DECLARE @wpress_sql_string nvarchar(4000);
DECLARE @linked_server nvarchar(4000);
DECLARE @linked_db nvarchar(255);
DECLARE @intCounter as int;

set @intCounter = 1;

while @intCounter <= (select max(id) from wpress.dim.wp_sites)
BEGIN
	Select 
	@linked_server = linked_server,
	@linked_db = linked_db
	from
	wpress.dim.wp_sites where id = @intCounter;

	BEGIN TRANSACTION T1;
	--delete old data from SQL Sever wp_posts table 
	delete from wp.wp_posts where wp_site_id = @intCounter;


	SET @OPENQUERY = 'SELECT * FROM OPENQUERY('+ @linked_server + ','''
	SET @wpress_sql_string = 'SELECT wp_posts.ID,
		wp_posts.post_author,
		wp_posts.post_date,
		null as post_date_gmt,
		wp_posts.post_content,
		wp_posts.post_title,
		wp_posts.post_excerpt,
		wp_posts.post_status,
		wp_posts.comment_status,
		wp_posts.ping_status,
		wp_posts.post_password,
		wp_posts.post_name,
		wp_posts.to_ping,
		wp_posts.pinged,
		wp_posts.post_modified,
		null as post_modified_gmt,
		wp_posts.post_content_filtered,
		wp_posts.post_parent,
		wp_posts.guid,
		wp_posts.menu_order,
		wp_posts.post_type,
		wp_posts.post_mime_type,
		wp_posts.comment_count
	FROM '+@linked_db+'.wp_posts'')' 

	-- insert wp_posts table into sql sever
	INSERT INTO wp.wp_posts
			   (ID
			   ,post_author
			   ,post_date
			   ,post_date_gmt
			   ,post_content
			   ,post_title
			   ,post_excerpt
			   ,post_status
			   ,comment_status
			   ,ping_status
			   ,post_password
			   ,post_name
			   ,to_ping
			   ,pinged
			   ,post_modified
			   ,post_modified_gmt
			   ,post_content_filtered
			   ,post_parent
			   ,guid
			   ,menu_order
			   ,post_type
			   ,post_mime_type
			   ,comment_count)
	EXEC (@OPENQUERY+@wpress_sql_string)

    UPDATE wp.wp_posts
	set wp_site_id = @intCounter where wp_site_id is null;

	print 'done'
set @intCounter = @intCounter+1

COMMIT TRANSACTION T1;
END;


--select * from wp.wp_posts

