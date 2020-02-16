<?php


namespace FOD\DBALClickHouse\Types;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\GuidType;

class UUIDType extends GuidType
{
	/**
	 * {@inheritdoc}
	 */
	public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
	{
		return "UUID";
	}
	/**
	 * {@inheritdoc}
	 */
	public function getName()
	{
		return DatableClickHouseType::TYPE_UUID;
	}

}