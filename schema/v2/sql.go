package v2

const triggerFunction = `create or replace function v2_update_head_timestamp() returns trigger as $end$
	declare
			rowTS text;
	begin
		if array_length(NEW.log, 1) = 0 then
			return null;
		end if;

		select max(v->>'createdAt') into rowTS from unnest(NEW.log) as v;
		if not found then
			return null;
		end if;

		update head_timestamp
			set timestamp = rowTS
			where timestamp < rowTS;
		return null;
	end;
$end$ language plpgsql`

const installTrigger = `create or replace trigger v2_update_head_timestamp
after insert or update on data
for each row execute function v2_update_head_timestamp()`
