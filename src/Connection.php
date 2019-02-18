<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\ParameterType;
use function strtoupper;
use function substr;
use function trim;

/**
 * ClickHouse Connection
 */
class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * {@inheritDoc}
     */
    public function executeUpdate($query, array $params = [], array $types = []) : int
    {
    	$query = str_replace("SET", "UPDATE", str_replace("UPDATE", "", $query));
    	foreach ($types as &$type)
    	{
    		$type = $type === 'float' ? 'integer' : $type;
	    }
    	return parent::executeUpdate("ALTER TABLE {$query}", $params, $types);
    }

	/**
	 * {@inheritDoc}
	 */
    public function delete($tableExpression, array $identifier, array $types = []) : int
    {
	    if (empty($identifier)) {
		    throw InvalidArgumentException::fromEmptyCriteria();
	    }

	    list($columns, $values, $conditions) = $this->gatherConditions($identifier);

	    return $this->executeUpdate(
		    'ALTER TABLE ' . $tableExpression . ' DELETE WHERE ' . implode(' AND ', $conditions),
		    $values,
		    is_string(key($types)) ? $this->extractTypeValues($columns, $types) : $types
	    );

    }
	/**
	 * Extract ordered type list from an ordered column list and type map.
	 *
	 * @param array $columnList
	 * @param array $types
	 *
	 * @return array
	 */
	private function extractTypeValues(array $columnList, array $types)
	{
		$typeValues = [];

		foreach ($columnList as $columnIndex => $columnName) {
			$typeValues[] = $types[$columnName] ?? ParameterType::STRING;
		}

		return $typeValues;
	}

	private function gatherConditions(array $identifiers)
	{
		$columns = [];
		$values = [];
		$conditions = [];

		foreach ($identifiers as $columnName => $value) {
			if (null === $value) {
				$conditions[] = $this->getDatabasePlatform()->getIsNullExpression($columnName);
				continue;
			}

			$columns[] = $columnName;
			$values[] = $value;
			$conditions[] = $columnName . ' = ?';
		}

		return [$columns, $values, $conditions];
	}
    /**
     * @throws DBALException
     */
    public function update($tableExpression, array $data, array $identifier, array $types = []) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @throws DBALException
     */
    public function setTransactionIsolation($level) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function getTransactionIsolation() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function getTransactionNestingLevel() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function transactional(\Closure $func) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function getNestTransactionsWithSavepoints() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function beginTransaction() : void
    {
	     return;
    }

    /**
     * @throws DBALException
     */
    public function commit() : void
    {
      return;
    }

    /**
     * @throws DBALException
     */
    public function rollBack() : void
    {
      return;
    }

    /**
     * @throws DBALException
     */
    public function createSavepoint($savepoint) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function releaseSavepoint($savepoint) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function rollbackSavepoint($savepoint) : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function setRollbackOnly() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @throws DBALException
     */
    public function isRollbackOnly() : void
    {
        throw DBALException::notSupported(__METHOD__);
    }
}
