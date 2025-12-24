"""
Config Loader Module
Loads and validates configuration from YAML/JSON files
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any, Union
from pydantic import BaseModel, Field, ValidationError


class CleaningRulesConfig(BaseModel):
    """Pydantic model for cleaning rules configuration"""
    columns: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    global_config: Dict[str, Any] = Field(default_factory=dict, alias='global')
    type_casting: Dict[str, str] = Field(default_factory=dict)
    validation: Dict[str, Any] = Field(default_factory=dict)
    output: Dict[str, Any] = Field(default_factory=dict)


class ETLConfig(BaseModel):
    """Pydantic model for main ETL configuration"""
    data_source: Dict[str, Any] = Field(default_factory=dict)
    data_destination: Dict[str, Any] = Field(default_factory=dict)
    database: Dict[str, Any] = Field(default_factory=dict)
    cleaning_rules_file: str = Field(default="config/cleaning_rules.yaml")
    logging: Dict[str, Any] = Field(default_factory=dict)


class ConfigLoader:
    """Utility class to load configuration files"""
    
    @staticmethod
    def load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load YAML configuration file
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Dictionary containing configuration
            
        Raises:
            FileNotFoundError: If file doesn't exist
            yaml.YAMLError: If YAML is invalid
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                config = yaml.safe_load(f)
                return config if config is not None else {}
            except yaml.YAMLError as e:
                raise yaml.YAMLError(f"Error parsing YAML file {file_path}: {e}")
    
    @staticmethod
    def load_json(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load JSON configuration file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Dictionary containing configuration
            
        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If JSON is invalid
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                config = json.load(f)
                return config
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(f"Error parsing JSON file {file_path}: {e}", e.doc, e.pos)
    
    @staticmethod
    def load_config(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load configuration file (auto-detect YAML or JSON)
        
        Args:
            file_path: Path to config file
            
        Returns:
            Dictionary containing configuration
        """
        file_path = Path(file_path)
        
        if file_path.suffix.lower() in ['.yaml', '.yml']:
            return ConfigLoader.load_yaml(file_path)
        elif file_path.suffix.lower() == '.json':
            return ConfigLoader.load_json(file_path)
        else:
            raise ValueError(f"Unsupported config file format: {file_path.suffix}")
    
    @staticmethod
    def load_etl_config(file_path: Union[str, Path] = "config/etl_config.yaml") -> ETLConfig:
        """
        Load and validate ETL configuration
        
        Args:
            file_path: Path to ETL config file
            
        Returns:
            Validated ETLConfig object
        """
        config_dict = ConfigLoader.load_config(file_path)
        try:
            return ETLConfig(**config_dict)
        except ValidationError as e:
            raise ValueError(f"Invalid ETL configuration: {e}")
    
    @staticmethod
    def load_cleaning_rules(file_path: Union[str, Path] = "config/cleaning_rules.yaml") -> CleaningRulesConfig:
        """
        Load and validate cleaning rules configuration
        
        Args:
            file_path: Path to cleaning rules config file
            
        Returns:
            Validated CleaningRulesConfig object
        """
        config_dict = ConfigLoader.load_config(file_path)
        try:
            return CleaningRulesConfig(**config_dict)
        except ValidationError as e:
            raise ValueError(f"Invalid cleaning rules configuration: {e}")


# Convenience functions
def load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Load YAML file - convenience function"""
    return ConfigLoader.load_yaml(file_path)


def load_json(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Load JSON file - convenience function"""
    return ConfigLoader.load_json(file_path)


def load_config(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Load config file (auto-detect format) - convenience function"""
    return ConfigLoader.load_config(file_path)


# Example usage
if __name__ == "__main__":
    # Test loading configuration
    try:
        etl_config = ConfigLoader.load_etl_config("config/etl_config.yaml")
        print("✓ ETL Config loaded successfully")
        print(f"  Input: {etl_config.data_source}")
        
        cleaning_rules = ConfigLoader.load_cleaning_rules("config/cleaning_rules.yaml")
        print("✓ Cleaning Rules loaded successfully")
        print(f"  Columns: {list(cleaning_rules.columns.keys())}")
        
    except Exception as e:
        print(f"✗ Error loading config: {e}")
