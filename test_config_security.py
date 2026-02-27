#!/usr/bin/env python3
"""Test configuration loading and validation."""

import sys
import os
from pathlib import Path

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pydantic import ValidationError
from app.infrastructure.config.config import Settings

def test_missing_required_env_vars():
    """Test that missing required environment variables raise ValidationError."""
    # Clear any relevant environment variables
    env_vars_to_clear = ['SECRET_KEY', 'MYSQL_PASSWORD']
    original_env = {}
    for var in env_vars_to_clear:
        original_env[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    
    try:
        # Should raise ValidationError because required fields are missing
        # Use _env_file=None to ignore .env which may have these values
        try:
            settings = Settings(_env_file=None)
            print("✗ FAIL: Expected ValidationError for missing required env vars, but got settings")
            return False
        except ValidationError as e:
            print(f"✓ PASS: ValidationError raised for missing env vars: {e.error_count()} error(s)")
            return True
    finally:
        # Restore original environment
        for var, value in original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

def test_valid_env_loading():
    """Test that settings load correctly with proper environment variables."""
    # Set required environment variables
    os.environ['SECRET_KEY'] = 'test-secret-key-1234567890'
    os.environ['MYSQL_PASSWORD'] = 'test-mysql-password'
    
    # Optional: set TUSHARE_TOKEN if needed
    os.environ['TUSHARE_TOKEN'] = 'test-tushare-token'
    
    # Also set MYSQL_HOST to match test expectations (since .env might override)
    os.environ['MYSQL_HOST'] = '127.0.0.1'
    os.environ['MYSQL_USER'] = 'root'
    os.environ['MYSQL_PORT'] = '3306'
    
    try:
        # Use _env_file=None to avoid loading .env file which may have different values
        settings = Settings(_env_file=None)
        
        # Verify required fields
        assert settings.secret_key == 'test-secret-key-1234567890', "SECRET_KEY mismatch"
        assert settings.mysql_password == 'test-mysql-password', "MYSQL_PASSWORD mismatch"
        
        # Verify fields with defaults
        assert settings.mysql_host == '127.0.0.1'
        assert settings.mysql_port == 3306
        assert settings.mysql_user == 'root'
        assert settings.algorithm == 'HS256'
        
        # Test derived properties
        expected_mysql_url = 'mysql+pymysql://root:test-mysql-password@127.0.0.1:3306'
        assert settings.mysql_url == expected_mysql_url, f"mysql_url mismatch: {settings.mysql_url}"
        
        print("✓ PASS: Settings loaded correctly with environment variables")
        return True
    except Exception as e:
        print(f"✗ FAIL: Settings loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        for var in ['SECRET_KEY', 'MYSQL_PASSWORD', 'TUSHARE_TOKEN', 'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PORT']:
            if var in os.environ:
                del os.environ[var]

def test_no_hardcoded_defaults():
    """Verify that sensitive fields have no default values."""
    from pydantic_core import PydanticUndefined
    
    # Using Pydantic model fields
    sensitive_fields = ['secret_key', 'mysql_password']
    for field_name in sensitive_fields:
        field = Settings.model_fields.get(field_name)
        if field:
            default = field.default
            # In Pydantic v2, no default means default is PydanticUndefined
            if default is not PydanticUndefined:
                print(f"✗ FAIL: {field_name} has a default value in model_fields: {default}")
                return False
        else:
            print(f"⚠ WARNING: {field_name} not found in model_fields")
    
    print("✓ PASS: No hardcoded defaults for sensitive fields")
    return True

if __name__ == '__main__':
    print("Testing Configuration Security...\n")
    
    results = []
    
    print("Test 1: Checking for hardcoded defaults")
    results.append(test_no_hardcoded_defaults())
    
    print("\nTest 2: Missing required environment variables")
    results.append(test_missing_required_env_vars())
    
    print("\nTest 3: Valid environment loading")
    results.append(test_valid_env_loading())
    
    print("\n" + "="*50)
    if all(results):
        print("All tests passed! ✓")
        sys.exit(0)
    else:
        print(f"Some tests failed: {sum(results)}/{len(results)} passed")
        sys.exit(1)
