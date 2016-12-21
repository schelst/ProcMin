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
    
 

                         
    cursor c_data_enrich is
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

    for d in c_data_enrich loop
     l_operation      := d.operation;
     l_id             := d.id;
     l_jn_entered_at  := d.jn_entered_at;
	 l_table_name     := d.table_name;
     
      -- nagaan welke kolom gewijzigd is
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
      
      
      
    
    end loop;

    --l_command := 'update '||l_table_name
    
  
end;
/