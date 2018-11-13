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
		/** @var ClickHouseTableBase $entityClass */
		$entityClass = $tableInfo->entityClass;

		return $entityClass;
	}

	/**
	 * @param Criteria $criteria
	 * @return array
	 * @throws \Doctrine\DBAL\DBALException
	 */
	public function findBy(Criteria $criteria)
	{
		$query = (new ClickHouseQuery($this->tableName, $this->connection))->select(["*"]);


		$query->where($criteria);

		$results = $query->execute();

		$return = new ArrayCollection();

		foreach ($results as $result)
		{
			$return->add($this->entityClass::newFromArray($result));
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

	/**
	 * @param $values
	 * @param Criteria $criteria
	 * @return bool false return if rows not found, because method can not be able resolve insert fields from $criteria
	 * @throws \Doctrine\DBAL\DBALException
	 */
	public function update($values, Criteria $criteria)
	{
		$start = microtime(true);
		$this->optimize();
		$find = false;
		foreach ((new ClickHouseQuery($this->tableName, $this->connection, true))->select(["*"])->where($criteria)->execute() as $result)
		{
			$find = true;
			$this->insert->add($this->entityClass::newFromArray(array_replace($result, $values)));
		}
		if(!$find){
			return false;
		}
		$this->flush(true);
		$this->logger->info("ClickHouse:", ['update' => microtime(true) - $start]);
		return true;
	}

	public function persist(ClickHouseTableBase $object)
	{
		$this->insert->add(clone $object);
	}

	public function flush($optimize = false)
	{
		if ($this->insert->count() === 0)
		{
			return true;
		}
		$fields = implode(',', array_keys($this->insert->first()->toSqlArray()));
		$start = microtime(true);
		$chunks = 0;
		foreach (array_chunk($this->insert->toArray(), 1000000) as $chunk)
		{
			$chunks++;
			$this->connection->exec("INSERT INTO {$this->tableName} ({$fields}) VALUES (" . implode("),(", array_map(function ($element)
				{
					return implode(",", $element->toSqlArray());
				}, $chunk)) . ")");
		}
		if($optimize === true)
		{
			$this->optimize();
		}
		$this->logger->info("ClickHouse:", ['time' => microtime(true) - $start, 'chunks' => $chunks, 'total count' => $this->insert->count()]);
		$this->insert = new ArrayCollection();

		return true;

	}

	public function optimize()
	{
		$this->connection->exec("OPTIMIZE TABLE " .  $this->tableName); // TODO проврека от двойного запроса
		$this->connection->exec("OPTIMIZE TABLE " . str_replace("_buffer", '', $this->tableName));
	}
}