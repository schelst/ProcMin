-- tabel opvullen met gegevens JN_PSN_PERSONEN
declare
  l_table_name      VARCHAR2(150);
  l_select_columns  VARCHAR2(500):='';
  l_column_name     VARCHAR2(150);
  l_columns_upd     VARCHAR2(500);
  l_id              NUMBER;
  l_unt_exists      varchar2(1);
  l_operation       varchar2(250);
  l_start_date      varchar2(50):='trunc(sysdate-365,''YYYY'')';
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

                         
    
    l_command   varchar2(1500);
    
    
    
begin
  delete PC_COMMANDS;
  
  for t in c_tables loop
   
    l_table_name := t.table_name;
    l_unt_exists := 0;
    
    l_command := 'select 1 from all_tab_columns where column_name=''UNT_ID'' and table_name='||l_table_name; 
    
    BEGIN
    execute immediate l_command into l_unt_exists;
    EXCEPTION WHEN OTHERS THEN
      l_unt_exists:=0;
    END;
    
    l_select_columns := 'jn_user, jn_entered_at, psn_id, decode(jn_operation,''INS'',''INSERT'', ''UPD'', ''UPDATE'', ''DEL'', ''DELETE'',jn_operation)||'' on '||replace(l_table_name,'JN_','')||''' operation,'''||l_table_name||''',id, jn_operation';
    
    IF l_table_name = 'JN_PSN_PERSONEN' THEN
      l_select_columns:=replace(l_select_columns, ', psn_id,', ', id,');
    END IF;
    
    if l_unt_exists=1 then
      l_select_columns := l_select_columns||', unt_id';
    else 
      l_select_columns := l_select_columns||', null unt_id';
    end if;
    
    
    l_command := 'insert into PC_COMMANDS (jn_user, jn_entered_at, psn_id, command, table_name, id, operation, unt_id) select '||l_select_columns||' from '||l_table_name||' where jn_operation=''INS'' and jn_entered_at>='||l_start_date;
    
    execute immediate l_command;
    
    Commit;
    
    l_command := 'insert into PC_COMMANDS (jn_user, jn_entered_at, psn_id, command, table_name, id, operation, unt_id,upd_columns) select '||l_select_columns||',''Gevalideerd'' from '||l_table_name||' where jn_operation=''UPD'' and gevalideerd_ind=''J'' and jn_entered_at>='||l_start_date;
    
    execute immediate l_command;
    
  end loop;  
end;
/
-- opkuisen data
delete pc_commands c
where operation='UPD'
and exists (select null from pc_commands where psn_id= c.psn_id and jn_entered_at < c.jn_entered_at and operation='INS')
/
delete pc_commands c
where operation='UPD'
and not exists (select null from pc_commands where psn_id=c.psn_id and operation='INS')
/
declare
  l_table_name      VARCHAR2(150);
  l_select_columns  VARCHAR2(500):='';
  l_column_name     VARCHAR2(150);
  l_columns_upd     VARCHAR2(500);
  l_id              NUMBER;
  l_unt_exists      varchar2(1);
  l_operation       varchar2(250);
  l_start_date      varchar2(50):='trunc(sysdate-365,''YYYY'')';
  l_selectprevRec   varchar2(250);
  l_jn_entered_at   timestamp;
    
  cursor c_tables is
        select table_name
        from all_tables
        where table_name like 'JN_%'
        and table_name IN (select 'JN_'||table_name
                           from all_constraints
                           where r_constraint_name='PSN_PK')
        and table_name != 'JN_PSN_PERSONEN'
        ;
    
    l_command   varchar2(1500);
    
begin
    
  for t in c_tables loop
   
    l_table_name := t.table_name;
    l_unt_exists := 0;
    
    l_command := 'select 1 from all_tab_columns where column_name=''UNT_ID'' and table_name='||l_table_name; 
    
    BEGIN
    execute immediate l_command into l_unt_exists;
    EXCEPTION WHEN OTHERS THEN
      l_unt_exists:=0;
    END;
    
    l_select_columns := 'jn_user, jn_entered_at, psn_id, decode(jn_operation,''INS'',''INSERT'', ''UPD'', ''UPDATE'', ''DEL'', ''DELETE'',jn_operation)||'' on '||replace(l_table_name,'JN_','')||''' operation,'''||l_table_name||''',id, jn_operation';
    
    if l_unt_exists=1 then
      l_select_columns := l_select_columns||', unt_id';
    else 
      l_select_columns := l_select_columns||', null unt_id';
    end if;
     
    l_command := 'insert into PC_COMMANDS (jn_user, jn_entered_at, psn_id, command, table_name, id, operation, unt_id) select '||l_select_columns||' from '||l_table_name||' where jn_operation=''INS'' and jn_entered_at>='||l_start_date;
     
    execute immediate l_command;
    
    Commit;
  
  end loop;  
end;
/