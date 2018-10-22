<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 05.06.2018
 * Time: 21:25
 */

namespace FOD\DBALClickHouse\Model;

/**
 * @Annotation
 * @Target("CLASS")
 */
class ClickHouseEntityTarget
{
	public $entityClass;
}
