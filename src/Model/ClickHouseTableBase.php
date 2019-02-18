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

		return $tableInfo->name;
	}

	/**
	 * @return \Doctrine\ORM\Mapping\Table
	 * @throws \Doctrine\Common\Annotations\AnnotationException
	 * @throws \ReflectionException
	 */
	public static function getTableInfo()
	{
		$reader = new AnnotationReader();
		$reflector = new \ReflectionClass(static::class);
		/** @var \Doctrine\ORM\Mapping\Table $tableInfo */
		return $reader->getClassAnnotation($reflector, '\Doctrine\ORM\Mapping\Table');
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

}