declare
  l_table_name      VARCHAR2(150);
  l_select_columns  VARCHAR2(500):='';
  l_column_name     VARCHAR2(150);
  l_columns_upd     VARCHAR2(500);
  l_id              NUMBER;
  l_unt_exists      varchar2(1);
  l_operation       varchar2(250);
  l_start_date      varchar2(50):='trunc(sysdate,''YYYY'')';
  l_selectprevRec   varchar2(250);
  l_jn_entered_at   timestamp;
    
  cursor c_tables is
        select table_name
        from all_tables
        where table_name like 'JN_%'
        and table_name IN (select 'JN_'||table_name
                           from all_constraints
                           where r_constraint_name='PSN_PK')
        and table_name = 'JN_PSN_PERSONEN'                   
        ;

                         
    cursor c_data_enrich (p_table_name varchar2) is
        select id, table_name, operation, JN_ENTERED_AT
        from pc_commands
        where enriched='N'
        ;
        
    l_command   varchar2(1500);
    
    function compareprevious (p_tableName in varchar2,
                              p_id IN number,
                              p_jn_entered_at IN timestamp
                             )
      return varchar2 is
      l_cp_command VARCHAR2(150);
      l_changed    VARCHAR2(5);
      l_return     VARCHAR2(500);
      l_columnName VARCHAR2(100);
      
      cursor c_columns(p_table_name varchar2) is
       select column_name
       from all_tab_columns
       where table_name=p_table_name;
      
    begin
    l_selectprevrec:='(select * from '||p_tableName||' where jn_entered_at=(select max(jn_entered_at) from '||p_tableName||' where jn_entered_at<'||p_jn_entered_at||'));';
    for u in c_columns(p_tablename) loop
      l_cp_command:='select case when a.'||l_columnName||' != b.'||l_columnName||' then ''C'' else ''NC'' end from '||p_tableName||' a join '||l_selectprevrec||' b on (a.id=b.id) where a.'||l_columnname||'!=b.'||l_columnname;
      
      execute immediate l_cp_command into l_changed;
      
      IF l_changed = 'C' THEN
        l_return:=l_return||' '||l_columnName||',';
      END IF;
      
    end loop;
     return l_return;
    end compareprevious;
    
begin
  delete PC_COMMANDS;
  
  for t in c_tables loop
   
    l_table_name := t.table_name;
   -- l_id         := t.id;
    l_unt_exists := 0;
    
    l_command := 'select 1 from all_tab_columns where column_name=''UNT_ID'' and table_name='||l_table_name; 
    
    BEGIN
    execute immediate l_command into l_unt_exists;
    EXCEPTION WHEN OTHERS THEN
      l_unt_exists:=0;
    END;
    
    l_select_columns := 'jn_entered_at, psn_id, decode(jn_operation,''INS'',''INSERT'', ''UPD'', ''UPDATE'', ''DEL'', ''DELETE'',jn_operation)||'' on '||replace(l_table_name,'JN_','')||''' operation,'''||l_table_name||''',id';
    
    IF l_table_name = 'JN_PSN_PERSONEN' THEN
      l_select_columns:=replace(l_select_columns, ', psn_id,', ', id,');
    END IF;
    
   -- dbms_output.put_line(l_command);
    
    if l_unt_exists=1 then
      l_select_columns := l_select_columns||', unt_id';
    else 
      l_select_columns := l_select_columns||', null unt_id';
    end if;
    
    
    l_command := 'insert into PC_COMMANDS (jn_entered_at, psn_id, command, table_name, id, unt_id) select '||l_select_columns||' from '||l_table_name||' where jn_entered_at>='||l_start_date;
    
    dbms_output.put_line(l_command);
    
    execute immediate l_command;
    
    Commit;
    
    for d in c_data_enrich(l_table_name) loop
     l_operation      := d.operation;
     l_id             := d.id;
     l_jn_entered_at  := d.jn_entered_at;
     
      -- nagaan welke kolom is gewijzigd
      if l_operation = 'UPD' then
        
             l_columns_upd := compareprevious(l_table_name, l_id, l_jn_entered_at);
             
             if l_columns_upd is not null
             then
               update pc_commands
               set upd_columns   = l_columns_upd,
                   operation     = l_operation
               where id          = l_id
               and table_name    = l_table_name
               and jn_entered_at = l_jn_entered_at;
             end if;
        
      end if;
      -- Andere enrichments
      
      
    
    end loop;
    --l_command := 'update '||l_table_name
    
  end loop;  
end;
/
select count(*)
from pc_commands
where psn_id is not null
/
drop table pc_commands
/
CREATE TABLE PC_COMMANDS
  (jn_entered_at date
  , psn_id number(38)
  , command varchar2(300)
  , table_name varchar2(250)
  , id number(38)
  , operation varchar2(3)
  , enriched varchar2(1) default 'N'
  , upd_columns varchar2(1000) default null
  , unt_id number default null);
 /
 delete pc_commands
 /
 select *
 from jn_psn_personen