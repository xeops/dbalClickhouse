<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 2019-02-18
 * Time: 09:46
 */

namespace FOD\DBALClickHouse\Types;


use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;

class FloatType extends \Doctrine\DBAL\Types\FloatType
{
	public function getBindingType()
	{
		return ParameterType::INTEGER;
	}
}