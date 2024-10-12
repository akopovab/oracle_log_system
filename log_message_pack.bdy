create or replace package body log_message_pack is

  g_osuser      v$session.osuser%type;
  g_oracle_user v$process.username%type;
  g_sid         v$session.sid%type;
  g_pid         v$process.pid%type;
  g_serial#     v$process.serial#%type;

  procedure log_message_(p_message        log_message.message%type
                        ,p_message_source log_message.message_source%type
                        ,p_message_type   log_message.message_type%type
                        ,p_message_dtime  log_message.dtime%type := systimestamp) is
    pragma autonomous_transaction;
  begin
    insert into log_message
      (id
      ,dtime
      ,message_type
      ,message
      ,message_source
      ,sid
      ,serial
      ,pid
      ,osuser
      ,oracle_user
      ,call_stack)
    values
      (log_message_pk_seq.nextval
      ,p_message_dtime --systimestamp
      ,p_message_type
      ,substr(p_message, 1, 2000)
      ,substr(p_message_source, 1, 2000)
      ,g_sid
      ,g_serial#
      ,g_pid
      ,g_osuser
      ,g_oracle_user
      ,substr(dbms_utility.format_call_stack, 1, 4000));
   commit;
  exception
    when others then
      -- вечный вопрос, что делать если произошла ошибка в процедура логирования =)
      -- один из подходов умолчать и сделать запись в alert.log через dbms_system.ksdwrt
      raise;-- я оставил raise, чтобы вы, если что заметили ошибку
  end;


  procedure info(p_message        log_message.message%type
                ,p_message_source log_message.message_source%type
                ,p_message_dtime  log_message.dtime%type := systimestamp) is
  begin
    log_message_(p_message, p_message_source, c_info_type, p_message_dtime);
  end;

  procedure warning(p_message        log_message.message%type
                   ,p_message_source log_message.message_source%type
                   ,p_message_dtime  log_message.dtime%type := systimestamp) is
  begin
    log_message_(p_message,
                 p_message_source,
                 c_warning_type,
                 p_message_dtime);
  end;

  procedure error(p_message        log_message.message%type
                 ,p_message_source log_message.message_source%type
                 ,p_message_dtime  log_message.dtime%type := systimestamp) is
  begin
    log_message_(p_message,
                 p_message_source,
                 c_error_type,
                 p_message_dtime);
  end;

  -- удаление пачками по типу сообщений с указанием дней за сколько оставить
  procedure clean_message_by_type_(p_message_type     log_message.message_type%type
                                  ,p_records_ttl_days pls_integer) is
    v_high_value_date date;
    v_high_value_type varchar2(1);
  begin
    for part in (select s.partition_name, s.high_value, sp.subpartition_name, sp.high_value as sub_partition_high_value
                   from user_tab_partitions s
                   left join user_tab_subpartitions sp on sp.partition_name = s.partition_name
                                                      and sp.table_name = s.table_name
                  where lower(s.table_name) = lower(c_table_name)) loop
      v_high_value_date := to_date(regexp_substr(substr(part.high_value, 1, 100), '([[:digit:]]{4})-([[:digit:]]{2})-([[:digit:]]{2})'), 'YYYY-MM-DD');
      v_high_value_type := substr(part.sub_partition_high_value, 2, 1);
      continue when v_high_value_date > trunc(sysdate) - p_records_ttl_days;
      continue when v_high_value_type != p_message_type;
      if v_high_value_type = c_error_type then
        execute immediate 'alter table ' || c_table_name || ' drop partition ' || part.partition_name;        
      else
        execute immediate 'alter table ' || c_table_name || ' drop subpartition ' || part.subpartition_name;
      end if;
    end loop;
  end;

  procedure clear_messages is
  begin
    clean_message_by_type_(c_info_type, 10); -- 10 дней на удаление info
    clean_message_by_type_(c_warning_type, 20); -- 20 дней на удаление warning
    clean_message_by_type_(c_error_type, 30); -- 30 дней на удаление error
  end;

begin
  -- 1 раз получаем инфу
  g_sid         := sys_context('userenv', 'sid');
  g_osuser      := sys_context('userenv', 'os_user');
  g_oracle_user := sys_context('userenv', 'session_user');

  select p.pid, p.serial#
    into g_pid, g_serial#
    from v$session s
    join v$process p
      on s.paddr = p.addr
   where s.sid = g_sid;
end;
