<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 05.06.2018
 * Time: 16:39
 */

namespace ClickHouseBundle\Model;


use Doctrine\Common\Collections\Criteria;
use Doctrine\ORM\Query\QueryExpressionVisitor;
use FOD\DBALClickHouse\Connection;

class ClickHouseQuery
{
	private $tableName = '';

	private $fields = [];
	/**
	 * @var Criteria[]
	 */
	private $wheres = [];

	private $orderBy = [];

	private $groupBy = [];

	private $limit = false;
	/**
	 * @var Connection
	 */
	private $connection;

	/**
	 * ClickHouseQuery constructor.
	 * @param string $tableName
	 * @param Connection $connection
	 */
	public function __construct(string $tableName, Connection $connection)
	{
		$this->tableName = $tableName;
		$this->connection = $connection;
	}

	/**
	 * @param string[] $fields
	 * @return $this
	 */
	public function select(array $fields)
	{
		$this->fields = $fields;

		return $this;
	}

	/**
	 * @param Criteria ...$criteries
	 * @return $this
	 */
	public function where(Criteria ... $criteries)
	{
		$this->wheres = $criteries;
		return $this;
	}

	/**
	 * @param Criteria $criteria
	 * @return $this
	 */
	public function addWhere(Criteria $criteria)
	{
		$this->wheres[] = $criteria;
		return $this;
	}

	/**
	 * @param string ...$fields
	 * @return $this
	 */
	public function groupBy(string ...$fields)
	{
		$this->groupBy = $fields;
		return $this;
	}

	/**
	 * @param string[] $fields
	 * @return $this
	 */
	public function orderBy(array $fields)
	{
		$this->orderBy = $fields;
		return $this;
	}

	/**
	 * @param int $from
	 * @param int $to
	 * @return $this
	 */
	public function limit(int $from, int $to)
	{
		$this->limit = "{$from}, {$to}";
		return $this;
	}

	/**
	 * @return array
	 * @throws \Doctrine\DBAL\DBALException
	 */
	public function execute()
	{
		$sql = "SELECT " . implode(',', $this->fields) . " FROM " . $this->tableName;

		$params = [];

		if (count($this->wheres) > 0) {
			$wheres = [];
			$sql .= " WHERE ";

			$visitor = new QueryExpressionVisitor([$this->tableName]);

			foreach ($this->wheres as $criteria) {
				if ($whereExpression = $criteria->getWhereExpression()) {
					$wheres[] = $visitor->dispatch($whereExpression)->__toString();
				}
			}

			foreach ($visitor->getParameters() as $parameter) {
				/** @var \Doctrine\ORM\Query\Parameter $parameter */
				$params[$parameter->getName()] = $parameter->getValue();
			}

			$sql .= "(" . implode(') AND ( ', $wheres) . ")";
		}

		if (count($this->groupBy) > 0) {
			$sql .= " GROUP BY " . implode(',', $this->groupBy);
		}

		if (count($this->orderBy) > 0) {
			$sql .= " ORDER BY " . implode(',', $this->orderBy);
		}

		if ($this->limit) {
			$sql .= "LIMIT {$this->limit}";
		}

		$stmt = $this->connection->prepare($sql);

		foreach ($params as $key => $value) {
			$type = null;
			if (is_int($value)) {
				$type = \PDO::PARAM_INT;
			} elseif (is_string($value) || is_float($value)) {
				$type = \PDO::PARAM_STR;
			} elseif (is_bool($value)) {
				$type = \PDO::PARAM_BOOL;
			}
			$stmt->bindValue($key, $value, $type);
		}

		$stmt->execute();

		return $stmt->fetchAll();

	}

}