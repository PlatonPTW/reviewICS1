-- Процедура импорта данных о сезонности клиентов из стейджинговой таблицы syn.SA_CustomerSeasonal
-- в таблицу syn.CustomerSeasonal
CREATE PROCEDURE syn.usp_ImportFileCustomerSeasonal
	@id_record INT
AS
BEGIN
	SET nocount ON;

	-- Объявление и инициализация переменных
	DECLARE
		@row_count INT = (SELECT COUNT(*) FROM syn.SA_CustomerSeasonal),
		@error_message VARCHAR(MAX),
		@data_source_id INT = 1;

	-- Проверка на корректность загрузки файла
	IF NOT EXISTS (
		SELECT
			1
		FROM syn.ImportFile AS f
		WHERE
			f.ID = @id_record
			AND f.FlagLoaded = CAST(1 AS BIT)
	)
	BEGIN
		SET @error_message = 'Ошибка при загрузке файла, проверьте корректность данных';
		RAISERROR(@error_message, 3, 1);
		RETURN;
	END;

	-- Создание временной таблицы для хранения валидных данных
	CREATE TABLE #customer_seasonal (
		ID_dbo_Customer INT NOT NULL,
		ID_CustomerSystemType INT NOT NULL,
		ID_Season INT NOT NULL,
		DateBegin DATE NOT NULL,
		DateEnd DATE NOT NULL,
		ID_dbo_CustomerDistributor INT NOT NULL,
		FlagActive BIT NOT NULL,
		MDT_DateCreate DATETIME2 NOT NULL CONSTRAINT DF_tmp_customer_seasonal_MDT_DateCreate DEFAULT(GETDATE()),
		MDT_ID_PrincipalCreatedBy INT NOT NULL CONSTRAINT DF_tmp_customer_seasonal_MDT_ID_PrincipalCreatedBy DEFAULT(original_login())
	);

	-- Заполнение временной таблицы валидными данными из стейджинговой таблицы
	INSERT INTO #customer_seasonal (
		ID_dbo_Customer,
		ID_CustomerSystemType,
		ID_Season,
		DateBegin,
		DateEnd,
		ID_dbo_CustomerDistributor,
		FlagActive
	)
	SELECT
		c.ID,
		cst.ID,
		s.ID,
		cs.DateBegin,
		cs.DateEnd,
		c_dist.ID,
		ISNULL(cs.FlagActive, 0)
	FROM syn.SA_CustomerSeasonal AS cs
	INNER JOIN dbo.Customer AS c
		ON c.UID_DS = cs.UID_DS_Customer
		AND c.ID_mapping_DataSource = @data_source_id
	INNER JOIN dbo.Season AS s
		ON s.Name = cs.Season
	INNER JOIN dbo.Customer AS c_dist
		ON c_dist.UID_DS = cs.UID_DS_CustomerDistributor
		AND c_dist.ID_mapping_DataSource = @data_source_id
	INNER JOIN syn.CustomerSystemType AS cst
		ON cs.CustomerSystemType = cst.Name
	WHERE
		ISDATE(cs.DateBegin) = 1
		AND ISDATE(cs.DateEnd) = 1;

	-- Создание временной таблицы для хранения невалидных данных
	CREATE TABLE #bad_inserted_rows (
		-- ... [Поля таблицы syn.SA_CustomerSeasonal]
		Reason VARCHAR(MAX)
	);

	-- Заполнение временной таблицы невалидными данными из стейджинговой таблицы
	INSERT INTO #bad_inserted_rows (
		-- ... [Поля таблицы syn.SA_CustomerSeasonal]
		Reason
	)
	SELECT
		cs.*,
		CASE
			WHEN c.ID IS NULL
				THEN 'UID клиента отсутствует в справочнике "Клиент"'
			WHEN c_dist.ID IS NULL
				THEN 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			WHEN s.ID IS NULL
				THEN 'Сезон отсутствует в справочнике "Сезон"'
			WHEN cst.ID IS NULL
				THEN 'Тип клиента отсутствует в справочнике "Тип клиента"'
			WHEN ISDATE(cs.DateBegin) = 0
				THEN 'Невозможно определить Дату начала'
			WHEN ISDATE(cs.DateEnd) = 0
				THEN 'Невозможно определить Дату окончания'
		END
	FROM syn.SA_CustomerSeasonal AS cs
	LEFT JOIN dbo.Customer AS c
		ON c.UID_DS = cs.UID_DS_Customer
		AND c.ID_mapping_DataSource = @data_source_id
	LEFT JOIN dbo.Customer AS c_dist
		ON c_dist.UID_DS = cs.UID_DS_CustomerDistributor
		AND c_dist.ID_mapping_DataSource = @data_source_id
	LEFT JOIN dbo.Season AS s
		ON s.Name = cs.Season
	LEFT JOIN syn.CustomerSystemType AS cst
		ON cst.Name = cs.CustomerSystemType
	WHERE
		c.ID IS NULL
		OR c_dist.ID IS NULL
		OR s.ID IS NULL
		OR cst.ID IS NULL
		OR ISDATE(cs.DateBegin) = 0
		OR ISDATE(cs.DateEnd) = 0;
		
	-- Мерджим данные из временной таблицы в основную
	MERGE syn.CustomerSeasonal AS t
	USING #customer_seasonal AS s
		ON s.ID_dbo_Customer = t.ID_dbo_Customer
		AND s.ID_Season = t.ID_Season
		AND s.DateBegin = t.DateBegin
	WHEN MATCHED AND t.ID_CustomerSystemType <> s.ID_CustomerSystemType THEN
	UPDATE
	SET 
		ID_CustomerSystemType = s.ID_CustomerSystemType,
		DateEnd = s.DateEnd,
		ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor,
		FlagActive = s.FlagActive
	WHEN NOT MATCHED THEN
	INSERT (
		ID_dbo_Customer,
		ID_CustomerSystemType,
		ID_Season,
		DateBegin,
		DateEnd,
		ID_dbo_CustomerDistributor,
		FlagActive
	)
	VALUES (
		s.ID_dbo_Customer,
		s.ID_CustomerSystemType,
		s.ID_Season,
		s.DateBegin,
		s.DateEnd,
		s.ID_dbo_CustomerDistributor,
		s.FlagActive
	);

	-- Информационное сообщение
	SET @error_message = CONCAT('Обработано строк: ', @row_count);
	RAISERROR(@error_message, 1, 1);

	-- Вывод отчетности о некорректных записях
	SELECT TOP (100)
		bir.Season AS [Сезон],
		bir.UID_DS_Customer AS [UID Клиента],
		bir.Customer AS [Клиент],
		bir.CustomerSystemType AS [Тип клиента],
		bir.UID_DS_CustomerDistributor AS [UID Дистрибьютора],
		bir.CustomerDistributor AS [Дистрибьютор],
		ISNULL(
			FORMAT(TRY_CAST(bir.DateBegin AS DATE), 'dd.MM.yyyy', 'ru-RU'),
			bir.DateBegin
		) AS [Дата начала],
		ISNULL(
			FORMAT(TRY_CAST(bir.DateEnd AS DATE), 'dd.MM.yyyy', 'ru-RU'),
			bir.DateEnd
		) AS [Дата окончания],
		bir.FlagActive AS [Активность],
		bir.Reason AS [Причина]
	FROM #bad_inserted_rows AS bir;

	RETURN;
END;