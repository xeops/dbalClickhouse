<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 05.06.2018
 * Time: 21:21
 */

namespace FOD\DBALClickHouse\Model;

use Doctrine\Common\Annotations\AnnotationReader;
use Doctrine\Common\Collections\ArrayCollection;
use Doctrine\Common\Collections\Criteria;
use FOD\DBALClickHouse\Connection;
use Psr\Log\LoggerInterface;

/**
 * Class ClickHouseRepository
 * @package FOD\DBALClickHouse\Model\Model
 */
class ClickHouseRepository
{

	/**
	 * @var LoggerInterface
	 */
	private $logger;
	/**
	 * @var Connection
	 */
	private $connection;

	/**
	 * @var string
	 */
	private $tableName;

	/**
	 * @var ArrayCollection
	 */
	private $insert;
	/**
	 * @var ClickHouseTableBase
	 */
	private $entityClass;

	public function __construct(LoggerInterface $logger, Connection $connection)
	{
		$this->logger = $logger;
		$this->connection = $connection;
		$this->entityClass = $this->getEntityClass();
		$this->tableName = $this->entityClass::getTableName();

		$this->insert = new ArrayCollection();
	}

	/**
	 * Метод Создания таблицы
	 * @throws \Doctrine\Common\Annotations\AnnotationException
	 * @throws \ReflectionException
	 */
	public function createTable()
	{
		$this->connection->getSchemaManager()->createTable($this->getEntityClass()::getTable());
	}

	/**
	 * @throws \Doctrine\Common\Annotations\AnnotationException
	 * @throws \ReflectionException
	 * @return ClickHouseTableBase
	 */
	private function getEntityClass()
	{
		$reader = new AnnotationReader();
		$reflector = new \ReflectionClass(static::class);

		/** @var \FOD\DBALClickHouse\Mapping\ClickHouseEntityTarget $tableInfo */
		$tableInfo = $reader->getClassAnnotation($reflector, '\FOD\DBALClickHouse\Mapping\ClickHouseEntityTarget');
		var_dump($tableInfo);
		/** @var ClickHouseTableBase $entityClass */
		$entityClass = $tableInfo->entityClass;

		return $entityClass;
	}

	/**
	 * @param Criteria[] $criteria
	 * @return array
	 * @throws \Doctrine\DBAL\DBALException
	 */
	public function findBy(array $criteria)
	{
		$query = (new ClickHouseQuery($this->tableName, $this->connection))->select(["*"]);

		foreach ($criteria as $criterion)
		{
			$query->addWhere($criterion);
		}

		$results = $query->execute();

		$return = new ArrayCollection();

		foreach ($results as $result)
		{
			$return->add($this->entityClass::newFromSql($result));
		}
		return $return->toArray();

	}

	/**
	 * @param Criteria[] $criteries
	 * @return array
	 * @throws \Doctrine\DBAL\DBALException
	 */
	public function findRawDataBy(array $criteries)
	{
		$query = (new ClickHouseQuery($this->tableName, $this->connection))->select(["*"]);
		foreach ($criteries as $criteria)
		{
			$query->addWhere($criteria);
		}
		return $query->execute();
	}

	/**
	 * @param string[] $fields
	 * @return ClickHouseQuery
	 */
	public function select(array $fields)
	{
		return (new ClickHouseQuery($this->tableName, $this->connection))->select($fields);
	}

	public function persist(ClickHouseTableBase $object)
	{
		$this->insert->add(clone $object);
	}

	public function flush()
	{
		if ($this->insert->count() === 0)
		{
			return true;
		}

		$fields = implode(',', array_keys($this->insert->first()->toSqlArray()));

		$insertStrings = [];

		foreach (array_chunk($this->insert->toArray(), 1000) as $chunk)
		{
			/** @var ClickHouseTableBase $item */
			foreach ($chunk as $item)
			{
				$insertString = [];

				foreach ($item->toSqlArray() as $value)
				{
					$insertString[] = (is_int($value) || is_float($value)) ? $value : "'" . str_replace("'", "\\'", $value) . "'";
				}
				$insertStrings[] = implode(",", $insertString);
			}

			/** @var string $sql */
			$sql = "INSERT INTO {$this->tableName} ({$fields}) VALUES (" . implode("),(", $insertStrings) . ")";

			$this->connection->exec($sql);
		}
		$this->insert = new ArrayCollection();

//		$this->connection->exec("OPTIMIZE TABLE {$this->tableName}"); //TODO понять наиболее оптимизированный запрос

		return true;

	}
}