<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 05.06.2018
 * Time: 16:04
 */

namespace FOD\DBALClickHouse\Model;


use Doctrine\Common\Annotations\AnnotationReader;

abstract class ClickHouseTableBase implements \JsonSerializable
{

	public static function getTableName()
	{
		$reader = new AnnotationReader();
		$reflector = new \ReflectionClass(static::class);
		/** @var \Doctrine\ORM\Mapping\Table $tableInfo */
		$tableInfo = $reader->getClassAnnotation($reflector, '\Doctrine\ORM\Mapping\Table');

		return $tableInfo->name . (isset($tableInfo->options['buffered']) && $tableInfo->options['buffered'] === true ? '_buffer' : '');
	}

	public static function getTable()
	{
		$reader = new AnnotationReader();
		$reflector = new \ReflectionClass(static::class);
		/** @var \Doctrine\ORM\Mapping\Table $tableInfo */
		$tableInfo = $reader->getClassAnnotation($reflector, '\Doctrine\ORM\Mapping\Table');

		$newTable = new \Doctrine\DBAL\Schema\Table($tableInfo->name);

		$props = $reflector->getProperties(\ReflectionProperty::IS_PRIVATE);

		$keys = [];
		$version = false;
		$eventDate = false;

		foreach ($props as $property)
		{
			/** @var \Doctrine\ORM\Mapping\Column $columnDefinition */
			$columnDefinition = $reader->getPropertyAnnotation($property, '\Doctrine\ORM\Mapping\Column');
			if (!$columnDefinition)
			{
				continue;
			}
			$newTable->addColumn($columnDefinition->name, $columnDefinition->type, $columnDefinition->options);

			if ($columnDefinition->unique === true)
			{
				$keys[] = $columnDefinition->name;
			}

			if ($reader->getPropertyAnnotation($property, '\FOD\DBALClickHouse\Mapping\EventDateColumn'))
			{
				$eventDate = $columnDefinition->name;
			}

			if ($reader->getPropertyAnnotation($property, '\FOD\DBALClickHouse\Mapping\VersionColumn'))
			{
				$version = $columnDefinition->name;
			}
		}

		$newTable->setPrimaryKey($keys);
		$newTable->addOption('engine', $tableInfo->schema);
		$newTable->addOption('buffered', $tableInfo->options['buffered'] ?? false);
		if ($eventDate)
		{
			$newTable->addOption('eventDateColumn', $eventDate);
		}
		if ($version)
		{
			$newTable->addOption('versionColumn', $version);
		}
		return $newTable;
	}

	/**
	 * @param array $array
	 * @return $this
	 */
	abstract public static function newFromArray($array);

	/**
	 * @return array
	 * @throws \Doctrine\Common\Annotations\AnnotationException
	 * @throws \ReflectionException
	 */
	abstract public function toSqlArray();

	/**
	 * @param array $array
	 * @return $this
	 */
	abstract public static function newFromSql($array);

}