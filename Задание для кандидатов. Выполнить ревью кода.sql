create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
AS
set nocount on
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	declare @ErrorMessage varchar(max) --1)Для объявления переменных  declare  используется один раз. Дополнительное объявление переменных через  declare  используется только, если необходимо использовать ранее объявленную переменную для определения значения объявляемой.Рекомендуется при объявлении типов не использовать длину поля  max 

-- Проверка на корректность загрузки
	if not exists (
	select 1 --2)В условных операторах весь блок кода смещается на 1 отступ
	from syn.ImportFile as f 
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit) 
	)
		begin --3) if  и  else  с  begin/end  должны быть на одном уровне
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных' 
			raiserror(@ErrorMessage, 3, 1)
			return
		end --4) if  и  else  с  begin/end  должны быть на одном уровне

	-- Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs 
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer --5)Все виды  join  пишутся с 1 отступом
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season --6)Все виды  join  пишутся с 1 отступом
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor --7)Все виды  join  пишутся с 1 отступом
			and c_dist.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name --8)Все виды  join  пишутся с 1 отступом
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null --9)Если есть  and , то он переносится на следующую строку и выравнивается на 1 табуляцию от  join 
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null --10)Если есть  and , то он переносится на следующую строку и выравнивается на 1 табуляцию от  join 

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	select
		cs.*
		,case
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"' --11)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"' --12)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"' --13)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"' --14)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала' --15)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания' --16)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность' --17)При написании конструкции с  case , необходимо, чтобы  when  был под  case  с 1 отступом,  then  с 2 отступами
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs --
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where c.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null 
		or try_cast(cs.DateEnd as date) is null 
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null 

	-- Обработка данных из файла
	merge into syn.CustomerSeasonal as cs --18)Перед названием таблицы, в которую осуществляется  merge ,  into  не указывается
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched 
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then --19) then  записывается на одной строке с  when , независимо от наличия дополнительных условий
		update --20)При написании  update/delete  запроса, необходимо использовать конструкцию с  from 
		set ID_CustomerSystemType = s.ID_CustomerSystemType 
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		raiserror(@ErrorMessage, 1, 1)

		--Формирование таблицы для отчетности
		select top 100
			bir.Season as 'Сезон'
			,bir.UID_DS_Customer as 'UID Клиента'
			,bir.Customer as 'Клиент'
			,bir.CustomerSystemType as 'Тип клиента'
			,bir.UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,bir.CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(bir.DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateBegin) as 'Дата начала'
			,isnull(format(try_cast(birDateEnd as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateEnd) as 'Дата окончания'
			,bir.FlagActive as 'Активность'
			,bir.Reason as 'Причина'
		from #BadInsertedRows as bir

		return
	end
end
