SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS `grammemes`;
DROP TABLE IF EXISTS `lemmas`;
DROP TABLE IF EXISTS `g_list`;
SET FOREIGN_KEY_CHECKS=1;

CREATE  TABLE IF NOT EXISTS `grammemes` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `alias` VARCHAR(10) DEFAULT NULL ,
  `description` VARCHAR(255) DEFAULT NULL ,
  `parent` INT(11) DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) ,
  UNIQUE INDEX `alias_UNIQUE` (`alias` ASC) ,
  INDEX `na_key` (`name` ASC, `alias` ASC, `id` ASC) ,
  INDEX `pn_key` (`parent` ASC, `name` ASC, `id` ASC) ,
  INDEX `pa_key` (`parent` ASC, `alias` ASC, `id` ASC) ,
  INDEX `pna_key` (`id` ASC, `name` ASC, `alias` ASC, `parent` ASC) ,
  INDEX `fk_parent_idx` (`parent` ASC) ,
  CONSTRAINT `fk_parent`
    FOREIGN KEY (`parent` )
    REFERENCES `grammemes` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;

CREATE  TABLE IF NOT EXISTS `lemmas` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(255) NOT NULL ,
  `parent` INT(11) DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) ,
  INDEX `name_key` (`id` ASC, `name` ASC) ,
  INDEX `parent_key` (`parent` ASC, `name` ASC) ,
  INDEX `all_key` (`id` ASC, `name` ASC, `parent` ASC) ,
  INDEX `fk_p_key_idx` (`parent` ASC) ,
  CONSTRAINT `fk_p_key`
    FOREIGN KEY (`parent` )
    REFERENCES `lemmas` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;

CREATE  TABLE IF NOT EXISTS `g_list` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `lemma_id` INT(11) NOT NULL ,
  `grammem_id` INT(11) NOT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `fk_lemma_id_idx` (`lemma_id` ASC) ,
  INDEX `fk_g_list_grammemes1_idx` (`grammem_id` ASC) ,
  INDEX `gr_key` (`lemma_id` ASC, `grammem_id` ASC) ,
  CONSTRAINT `fk_lemma_id`
    FOREIGN KEY (`lemma_id` )
    REFERENCES `lemmas` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_g_list_grammemes1`
    FOREIGN KEY (`grammem_id` )
    REFERENCES `grammemes` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
