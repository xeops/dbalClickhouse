<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 05.06.2018
 * Time: 20:57
 */

namespace ClickHouseBundle\Mapping;


use Doctrine\ORM\Mapping\Annotation;

/**
 * @Annotation
 * @Target("PROPERTY")
 */
final class VersionColumn implements Annotation
{

}